module Main where

import Lib
import Text.CSV
-- Para pegar o path do executavel
import System.Environment.FindBin
import System.Directory (createDirectoryIfMissing, doesFileExist)
import System.FilePath.Posix (takeDirectory)
import qualified Data.Aeson as DA
import qualified Data.ByteString.Lazy as BS
import Data.Text.Encoding (decodeUtf8)
import qualified Text.PrettyPrint.Boxes as PB
import Data.List
import qualified Graphics.Rendering.Chart.Easy          as C
import qualified Graphics.Rendering.Chart.Backend.Cairo as C
import Data.Time
import Data.Time.LocalTime
import qualified Data.Map.Strict as Map
import qualified Network.HTTP.Download as Download
import Network.HTTP.Types.Status (statusCode)
import Network.HTTP.Client          -- package http-client
import Network.HTTP.Client.TLS      -- package http-client-tls
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy as ByteString
import qualified Codec.Compression.GZip as GZip
import Graphics.Image.IO as Graphics
import qualified Text.Parsec.Error as TErr
import Text.Printf
import qualified Data.FuzzySet as FZ
import qualified Data.Text as T
import qualified Text.Read as TR
import qualified Data.HashMap.Strict as HM
import qualified Data.Vector as DV
import qualified Data.Scientific as DataScientific

-- Mensagens de sucesso e erro
teveErro _ = putStrLn "CSV possui erros. Impossível continuar a operação."
municipioNaoEncontrado = "Municipio NÃO Encontrado. Ou não existe, ou não possui casos de COVID até o momento"
-- Numero total de colunas do CSV do Brasil.IO
totalColunas = 18
-- Quantos dias a média móvel calcula
diasMediaMovel = 7
-- Horario do Fechamento do Boletim do Brasil.IO
horarioFechamentoBrasilIo = "20:00:00"
-- Strings a serem adicionadas aos arquivos gerados
imgCasosPath = "casos"
imgNovosCasosPath = "novos-casos"
imgObitosPath = "obitos"
imgNovosObitosPath = "novos-obitos"
-- Índices da tabela
colunaIbgeId = 2
colunaCasosConfirmados = 8
colunaCasos100k = 9
colunaObitosConfirmados = 12
colunaObitos100k = 11
colunaNovosCasos = 16
colunaNovosObitos = 17
colunaPopulacao = 4
-- Colunas a serem exibidas na tabela
colunasTabela = [colunaIbgeId, colunaCasosConfirmados, colunaObitosConfirmados, colunaNovosCasos, colunaNovosObitos, colunaPopulacao]

-- URL para o data set do Brasil.IO
urlDataSet = "https://data.brasil.io/dataset/covid19/caso_full.csv.gz"
urlMunicipioIdGithub = "https://raw.githubusercontent.com/wcota/covid19br/master/cities_info.csv"
urlMunicipiosVizinhosGithub =  "https://raw.githubusercontent.com/rafaelcalpena/municipios-vizinhos/master/vizinhos-ajustado.json"

-- Configuracoes do grafico
casosTitulo nome municipioId = "Casos em " ++ nome ++ " - " ++ municipioId
casosLegenda = "Casos"
novosCasosTitulo nome municipioId = "Novos Casos em " ++ nome ++ " - " ++ municipioId
novosCasosLegenda = "Novos Casos"
mediaMovelNovosCasos diasMediaMovel = "Média móvel de " ++ (show diasMediaMovel) ++ " dias"
obitosTitulo nome municipioId = "Óbitos em " ++ nome ++ " - " ++ municipioId
obitosLegenda = "Óbitos"
novosObitosTitulo nome municipioId = "Novos Óbitos em " ++ nome ++ " - " ++ municipioId
novosObitosLegenda = "Novos Óbitos"
mediaMovelNovosObitos diasMediaMovel = "Média móvel de " ++ (show diasMediaMovel) ++ " dias"

-- Maximo de sugestoes para uma pesquisa
maxSugestoesNome = 5

-- Recebe uma lista e soma todos os numeros do intervalo de índices i até f ao acumulador
-- Caso i < 0, soma o valor 0 para continuar a operação
somaSegura :: Num a => [a] -> Int -> Int -> a -> a
somaSegura dados i f acc = if (prox /= f + 1) 
    then somaSegura dados prox f (acc + valor) 
    else acc where
    valor = if (i < 0) then 0 else (dados !! i)
    prox = i+1

-- Junta indexes com a lista
comIndex :: [a] -> [(Int, a)]
comIndex = zip [0..]

-- Usando GZip, descompacta um arquivo gz no path de destino escolhido
descompactar :: FilePath -> FilePath -> IO ()
descompactar arq target = do
    content <- fmap GZip.decompress (BL.readFile arq)
    criarArquivo target content

-- Calcula a média móvel para uma lista de Integers a partir do índice i com q índices anteriores
mediaMovel :: [Integer] -> Int -> Int -> Double
mediaMovel dados i q = fromIntegral(numerador) / fromIntegral(denominador) :: Double where
    numerador = somaSegura dados inicio fim 0
    denominador = q
    inicio = i + 1 - q
    fim = i + 1

-- Mapa (Cód. IBGE Id => [Lista de Linhas do Município])
type MapaAcc = Map.Map Field [Record];

-- Recebe uma linha do csv e adiciona na chave correspondente
addLinha :: Record -> MapaAcc -> MapaAcc
addLinha l mapa
    | mapaEstaVazio = novoMapa
    | otherwise = (case valorMapa of
        -- Município já foi adicionado ao mapa, adicionar linha a lista
        Just valor -> Map.insert ibgeId (l : valor) mapa
        -- Município nao foi adicionado, adicionar município e lista
        Nothing -> Map.insert ibgeId [l] mapa) where
    ibgeId = l !! 1        
    valorMapa = Map.lookup ibgeId mapa
    novoMapa = Map.fromList [(ibgeId, [l])]
    mapaEstaVazio = (Map.size mapa) == 0

-- Recebe uma tabela CSV e a transforma em um MapaAcc com cidades como chave para lookup eficiente
agruparPorCidade :: [Record] -> MapaAcc -> MapaAcc
agruparPorCidade [] evts = evts
agruparPorCidade (l:ls) evts = agruparPorCidade ls (addLinha l evts)

-- Recebe uma lista de tuplas (Ibge ID, [linha]) e escreve os arquivos Json para cada uma delas
gerarArquivosJson :: [(Field, [Record])] -> FilePath -> IO ()
gerarArquivosJson [] _ = do
    return ()
gerarArquivosJson (t:acc) pasta = do
    putStrLn ("Faltam " ++ (show ((length acc) + 1)) ++ " arquivos .json")
    criarArquivo (pasta ++ "/" ++ (fst t) ++ ".json") (DA.encode (snd t))
    gerarArquivosJson acc pasta

-- Cria um arquivo (e diretorio pai, caso nao exista) com conteúdo como ByteString
criarArquivo :: FilePath -> ByteString.ByteString -> IO ()
criarArquivo path conteudo = do
    createDirectoryIfMissing True (takeDirectory path)
    BS.writeFile path conteudo

-- Extrai uma coluna (índice i) a partir de uma tabela CSV
extrairColuna :: Int -> [Record] -> [String] -> [String]
extrairColuna i [] acc = acc
extrairColuna i (r:lss) acc = extrairColuna i lss (acc ++ [r !! i])

-- Filtra colunas de uma linha
filtrarColunas :: [Int] -> Record -> Record -> Record
filtrarColunas [] _ acc = acc
filtrarColunas (i:is) r acc = filtrarColunas is r (acc ++ [r !! i])

-- Recebe lista de índices, lista de listas e acumulador e retorna lista de colunas extraidas
extrairColunas :: [Int] -> [Record] -> [[String]] -> [[String]]
extrairColunas is [] acc = acc
extrairColunas is (r:lss) acc = extrairColunas is lss (acc ++ [itemFiltrado]) where
    itemFiltrado = filtrarColunas is r []

-- Baixa uma URL em um arquivo local
-- Créditos: https://stackoverflow.com/questions/60793414/how-to-download-internet-file-locally-in-haskell
baixarDados url path = do
    -- Cria uma requisicao
    req <- parseRequest url    
    -- Cria um connection manager
    manager <- newManager tlsManagerSettings
    -- Realiza a requisicao
    r <- httpLbs req manager
    -- Recebe o conteudo como lazy ByteString
    let conteudo = responseBody r
    -- Escreve no arquivo
    criarArquivo path conteudo    

-- Mostra o horário na tela
mostrarHorario :: IO ()
mostrarHorario = do
    inicio <- getCurrentTime
    print inicio

-- Carrega o CSV, imprime progresso na tela e retorna o conteudo
carregarCSVCovid :: FilePath -> IO String
carregarCSVCovid arquivoCOVID = do
    putStrLn "Carregando o arquivo de casos de COVID"
    conteudo <- readFile (arquivoCOVID)
    putStrLn "Arquivo Carregado"
    mostrarHorario
    return conteudo

-- Faz o parsing do CSV, imprime progresso na tela e retorna um erro ou o proprio CSV
parseCSVCovid :: FilePath -> String -> IO (Either TErr.ParseError CSV)
parseCSVCovid arquivoCOVID conteudo = do
    putStrLn "Realizando o parsing do CSV. Isso poderá levar alguns minutos"
    let csv = parseCSV arquivoCOVID conteudo
    -- Verifica se foi bem sucedido
    mostrarHorario  
    return csv

-- Filtra o cabecalho e linhas que nao seguem o padrao da tabela COVID
filtrarResultados :: CSV -> IO CSV
filtrarResultados resultado = do
    putStrLn "Filtrando os resultados"  
    -- A primeira linha é o cabecalho, precisamos tirá-la
    let dados = tail resultado
    putStrLn $ "Obtidas as linhas, total de " ++ show (length dados) ++ " registros"
    -- Filtrar linhas em branco (ultima linha do csv)
    let dadosFiltrados = filter (\l -> length l == totalColunas) dados
    putStrLn $ "Registros filtrados restantes " ++ (show (length dadosFiltrados))
    putStrLn "Resultados foram filtrados"
    mostrarHorario
    return dadosFiltrados

-- Gera um arquivo JSON para cada municipio do Brasil
gerarJsonsCOVID :: ListaMapa -> FilePath -> IO ()
gerarJsonsCOVID lista pastaOutput = do
    putStrLn ("Gerando " ++ show (length lista) ++ " arquivos JSON")
    mostrarHorario
    gerarArquivosJson lista pastaOutput   
    putStrLn "Geracao de arquivos JSON concluída" 
    mostrarHorario    
    return ()

type ListaMapa = [(Field, [Record])]

-- Realiza o agrupamento de municipios e imprime informacoes na tela
realizarAgrupamento :: FilePath -> CSV -> IO (ListaMapa)
realizarAgrupamento pastaOutput resultado = do
    putStrLn "Separando os casos por cidades"
    mostrarHorario

    putStrLn "Agrupando por cidades..."

    let accMapa = agruparPorCidade resultado Map.empty
    let lista = Map.toList accMapa

    putStrLn ("Agrupamento concluido. " ++ show (length lista) ++ " municípios no registro.")
    return lista

-- Inicializar os procedimentos de 
-- carregamento, parsing, extracao do cabecalho
-- filtragem das linhas, agrupamento por município
-- e geracao de arquivos JSON
parsingSucesso :: FilePath -> CSV -> IO ()
parsingSucesso pastaOutput resultado = do
    putStrLn "Parsing realizado com sucesso"
    dadosFiltrados <- filtrarResultados resultado
    lista <- realizarAgrupamento pastaOutput dadosFiltrados 
    gerarJsonsCOVID lista pastaOutput

preIndexarDadosCovid :: FilePath -> FilePath -> IO ()
preIndexarDadosCovid pastaOutput arquivoCOVID = do
    conteudo <- carregarCSVCovid arquivoCOVID
    csv <- parseCSVCovid arquivoCOVID conteudo
    either teveErro (parsingSucesso pastaOutput) csv  

-- Interface para perguntar o codigo IBGE ao usuario
perguntaCodIBGE :: IO (String)
perguntaCodIBGE = do
    putStrLn "Digite o nome ou código IBGE do município"
    municipioId <- getLine
    return municipioId

-- Retorna se o arquivo do municipio existe
procuraArquivoMunicipio :: FilePath -> IO (Bool) 
procuraArquivoMunicipio municipioFile = do
    jsonExiste <- doesFileExist municipioFile
    return jsonExiste

-- Carrega os dados de um municipio. Recebe uma funcao sucesso
carregarDadosMunicipio :: String -> FilePath -> ([Record] -> IO ()) -> IO ()
carregarDadosMunicipio municipioId pastaOutput sucesso = do
    let municipioFile = getMunicipioFile municipioId pastaOutput
    resultadoArq <- BS.readFile municipioFile
    let Just dados = DA.decode resultadoArq :: Maybe [Record]
    sucesso dados
    return ()

imprimirVizinho :: Map.Map Field Field -> String -> IO ()
imprimirVizinho municipioIds id = do
    putStrLn ("(ID: " ++ id ++ ") - " ++ nome) where
        (Just nome) = Map.lookup id municipioIds

-- Exibe a tabela de Casos e Obitos de COVID para um municipio no terminal
exibirTabela :: String -> String -> [Record] -> [String] -> Map.Map Field Field -> IO ()
exibirTabela municipioId nome tabela vizinhos municipioIds = do
    putStrLn ("* Município - " ++ nome ++ ":")
    putStrLn ("* Código IBGE - " ++ municipioId)

    -- Obtemos as tuplas (LocalTime, Valor)
    let tuplaDataNovosCasos = map (converterParaTupla 3) tabela
    let tuplaDataNovosObitos = map (converterParaTupla 4) tabela

    -- Calculamos a mediaMovel de casos e obitos
    let mediaMovelObitos = getMediaMovel tuplaDataNovosObitos
    let mediaMovelCasos = getMediaMovel tuplaDataNovosCasos

    -- Por algum motivo o Brasil.IO nao exibe obitos/100k na tabela
    let numerador l = read(l !! 2) :: Double
    let denominador l = read(l !! 5) :: Double
    let obitos100k = map (\linha -> (numerador linha) / ((denominador linha)/ 100000) ) tabela

    -- E casos/100k possui linhas faltantes (quebra o parsing em alguns municipios). Precisei reimplementar
    let numeradorCasos l = read(l !! 1) :: Double
    let casos100k = map (\linha -> (numeradorCasos linha) / ((denominador linha)/ 100000) ) tabela


    -- Renderizamos a tabela
    let headers = ["DATA", "CASOS", "ÓBITOS", "NOVOS CASOS", "NOVOS ÓBITOS", "POPULAÇÃO"]
    let colsMediaMovel = [("M.M. CASOS" : (map (printf "%.2f") mediaMovelCasos))] ++ [("M.M. ÓBITOS" : (map (printf "%.2f") mediaMovelObitos))] ++ [("CASOS/100k": (map (printf "%.2f") casos100k))] ++ [("ÓBITOS/100k": (map (printf "%.2f") obitos100k))]
    let cols = transpose ((take 6 headers) : tabela) ++ colsMediaMovel

    let configTabela = PB.vcat PB.right . map PB.text
    let fn = map configTabela cols
    let box = PB.hsep 2 PB.right fn
    let renderizar = PB.render box
    putStrLn renderizar  

    putStrLn "Municípios Vizinhos: "

    mapM_ (imprimirVizinho municipioIds) vizinhos

    return ()

-- Gera um grafico padrao (plot de pontos)
-- Baseado nos docs da documentacao da biblioteca Chart
gerarGraficoPadrao :: FilePath -> String -> String -> [(LocalTime, Integer)] -> IO ()
gerarGraficoPadrao arq titulo legenda dados = do    
    C.toFile C.def arq $ do
        C.layout_title C..= titulo
        C.layout_background C..= C.solidFillStyle (C.opaque C.white)
        C.layout_foreground C..= (C.opaque C.black)
        C.plot (C.points legenda dados)

-- Gera um grafico com média móvel (plot de pontos + linha de tendencia)
-- Baseado nos docs da documentacao da biblioteca Chart
gerarGraficoMedia :: FilePath -> String -> String -> [(LocalTime, Double)] -> String -> [(LocalTime, Double)] -> IO ()
gerarGraficoMedia arq titulo legenda dados legendaMedia dadosMediaMovel = do
    C.toFile C.def arq $ do
        C.layout_title C..= titulo
        C.layout_background C..= C.solidFillStyle (C.opaque C.white)
        C.layout_foreground C..= (C.opaque C.black)
        C.plot (C.points titulo dados )
        C.plot (C.line legendaMedia [dadosMediaMovel])    

data TipoGrafico = Casos | Obitos deriving (Eq)

-- gerarVizinhos100k :: FilePath -> String -> IO ()
gerarVizinho100k pastaOutput municipioIds tipo id = do
    let municipioFile = getMunicipioFile id pastaOutput
    jsonExiste <- procuraArquivoMunicipio municipioFile  
    if (jsonExiste) then do
        resultadoArq <- BS.readFile municipioFile
        let Just dados = DA.decode resultadoArq :: Maybe [Record]
        -- Nome do municipio
        let Just nome = Map.lookup id municipioIds

        -- Filtra apenas as colunas desejadas da tabela
        let tabela = extrairColunas colunasTabela dados []   

        let numeradorCasos l = read(l !! 1) :: Double
        let denominador l = read(l !! 5) :: Double
        let casos100k = map (\linha -> (numeradorCasos linha) / ((denominador linha)/ 100000) ) tabela
        let colunaDatas = (transpose tabela) !! 0
        let datas = map obterData tabela
        let tuplaDataCasos100k = zip datas casos100k

        let numerador l = read(l !! 2) :: Double
        let denominador l = read(l !! 5) :: Double
        let obitos100k = map (\linha -> (numerador linha) / ((denominador linha)/ 100000) ) tabela
        let colunaDatas = (transpose tabela) !! 0
        let datas = map obterData tabela
        let tuplaDataObitos100k = zip datas obitos100k

        return (id, nome, if (tipo == Casos) then tuplaDataCasos100k else tuplaDataObitos100k)
    else do
        return (id, "", [])  



gerarGrafico100k :: FilePath -> String -> String -> [(LocalTime, Double)] -> [String] -> FilePath -> Map.Map Field Field -> TipoGrafico -> IO ()
gerarGrafico100k arq titulo legenda dados vizIds pastaOutput municipioIds tipo = do
    vizinhos <- mapM (gerarVizinho100k pastaOutput municipioIds tipo) vizIds
    C.toFile C.def arq $ do
        C.layout_title C..= titulo
        C.layout_background C..= C.solidFillStyle (C.opaque C.white)
        C.layout_foreground C..= (C.opaque C.black)
        mapM_ (\ (id, nome, dados) -> C.plot (C.points nome dados)) vizinhos
        C.plot (C.points legenda dados)
        

-- Gera os gráficos de casos, obitos, novos casos e novos obitos para um determinado municipio
gerarGraficos :: [Record] -> (String -> FilePath) -> String -> String -> Map.Map Field Field -> [String] -> FilePath -> IO ()
gerarGraficos tabela mkNomeArqGrafico nome municipioId municipioIds vizIds pastaOutput = do

    let tuplaDataCasos = map (converterParaTupla 1) tabela
    gerarGraficoPadrao (mkNomeArqGrafico imgCasosPath) (casosTitulo nome municipioId) casosLegenda tuplaDataCasos

    let tuplaDataNovosCasos = map (converterParaTupla 3) tabela
    let tuplaDataNovosCasosDouble = map sndParaDouble tuplaDataNovosCasos
    let tuplaDataMediaMovelCasos = mkTuplaMediaMovel tuplaDataNovosCasos
    gerarGraficoMedia (mkNomeArqGrafico imgNovosCasosPath) (novosCasosTitulo nome municipioId) novosCasosLegenda tuplaDataNovosCasosDouble (mediaMovelNovosCasos diasMediaMovel) tuplaDataMediaMovelCasos

    let tuplaDataObitos = map (converterParaTupla 2) tabela
    gerarGraficoPadrao (mkNomeArqGrafico imgObitosPath) (obitosTitulo nome municipioId) obitosLegenda tuplaDataObitos

    let tuplaDataNovosObitos = map (converterParaTupla 4) tabela
    let tuplaDataNovosObitosDouble = map (sndParaDouble) tuplaDataNovosObitos
    let tuplaDataMediaMovelObitos = mkTuplaMediaMovel tuplaDataNovosObitos
    gerarGraficoMedia (mkNomeArqGrafico imgNovosObitosPath) (novosObitosTitulo nome municipioId) novosObitosLegenda tuplaDataNovosObitosDouble (mediaMovelNovosObitos diasMediaMovel) tuplaDataMediaMovelObitos

    let numeradorCasos l = read(l !! 1) :: Double
    let denominador l = read(l !! 5) :: Double
    let casos100k = map (\linha -> (numeradorCasos linha) / ((denominador linha)/ 100000) ) tabela
    let colunaDatas = (transpose tabela) !! 0
    let datas = map obterData tabela
    let tuplaDataCasos100k = zip datas casos100k
    gerarGrafico100k (mkNomeArqGrafico "casos100k") ("Casos/100k - " ++ nome) (nome) tuplaDataCasos100k vizIds pastaOutput municipioIds Casos

    -- Por algum motivo o Brasil.IO nao exibe obitos/100k na tabela
    let numerador l = read(l !! 2) :: Double
    let obitos100k = map (\linha -> (numerador linha) / ((denominador linha)/ 100000) ) tabela
    let colunaDatas = (transpose tabela) !! 0
    let datas = map obterData tabela
    let tuplaDataObitos100k = zip datas obitos100k
    gerarGrafico100k (mkNomeArqGrafico "obitos100k") ("Obitos/100k - " ++ nome) (nome) tuplaDataObitos100k vizIds pastaOutput municipioIds Obitos



-- Mostra para o usuário os arquivos dos gráficos criados
mostrarGraficos :: (String -> String) -> IO ()
mostrarGraficos municipioGrafico = do
    Graphics.displayImageFile Graphics.defaultViewer (municipioGrafico "obitos100k")
    Graphics.displayImageFile Graphics.defaultViewer (municipioGrafico "casos100k")
    Graphics.displayImageFile Graphics.defaultViewer (municipioGrafico imgNovosObitosPath)  
    Graphics.displayImageFile Graphics.defaultViewer (municipioGrafico imgObitosPath)
    Graphics.displayImageFile Graphics.defaultViewer (municipioGrafico imgNovosCasosPath)
    Graphics.displayImageFile Graphics.defaultViewer (municipioGrafico imgCasosPath)

    return ()

-- Adiciona o horario de fechamento do Brasil.IO a data informada
dataHorarioBrasilIo :: String -> String
dataHorarioBrasilIo d = d ++ " " ++ horarioFechamentoBrasilIo

-- Recebe uma linha da tabela e retorna o parsing da data
obterData :: Record -> LocalTime
obterData item = parseTimeOrError True defaultTimeLocale "%F %H:%M:%S" dataFinal where
    dataFinal = dataHorarioBrasilIo (item !! 0)

-- Converte uma coluna de uma lista para a tupla (LocalTime, Integer)
converterParaTupla :: Int -> Record -> (LocalTime, Integer)
converterParaTupla indice item = (obterData item, read (item !! indice) :: Integer)

converterParaTuplaDouble :: Int -> Record -> (LocalTime, Double)
converterParaTuplaDouble indice item = (obterData item, read (item !! indice) :: Double)

-- Converte o segundo argumento de uma tupla para Double
sndParaDouble :: Integral b => (a, b) -> (a, Double)
sndParaDouble i = (fst i, fromIntegral(snd i) :: Double)

-- Obtem uma tupla de média móvel dos valores das tuplas de entrada
mkTuplaMediaMovel :: [(LocalTime, Integer)] -> [(LocalTime, Double)]
mkTuplaMediaMovel tuplaData = tuplaDataMediaMovelCasos where
    listaNovos = reverse (map snd tuplaData)
    tuplaIndiceNovosCasos = comIndex listaNovos
    mediaMovelFn (i, t) = mediaMovel listaNovos i diasMediaMovel
    mediaMovelCasos = reverse (map mediaMovelFn  tuplaIndiceNovosCasos)
    tuplaDataMediaMovelCasos = zip (map fst tuplaData) mediaMovelCasos

-- Obtem a média móvel para os valores das tuplas
getMediaMovel :: [(LocalTime, Integer)] -> [Double]
getMediaMovel tuplaData = map snd (mkTuplaMediaMovel tuplaData)

-- Gera o nome da imagem para o grafico com a pasta, id do municipio e tipo do gráfico
gerarNomeArqGrafico :: FilePath -> String -> String -> String
gerarNomeArqGrafico pastaOutput municipioId tipo = pastaOutput ++ "/" ++ municipioId ++ "_" ++ tipo ++ ".png"

-- Hack para converter de Scientific String para Integer
trimNum :: String -> String -> String
trimNum acc [] = acc
trimNum acc (n:ns) = if (n == '.') then acc else trimNum (acc ++ [n]) ns

-- Exibe tabela e gráficos para um município escolhido
exibirMunicipio :: FilePath -> String -> Map.Map Field Field -> DA.Object -> [Record] -> IO ()
exibirMunicipio pastaOutput municipioId municipioIds raiz dados = do

    -- Funcao para criar o nome do arquivo dos graficos
    let mkNomeArqGrafico = gerarNomeArqGrafico pastaOutput municipioId

    -- Nome do municipio
    let Just nome = Map.lookup municipioId municipioIds

    -- Filtra apenas as colunas desejadas da tabela
    let tabela = extrairColunas colunasTabela dados []

    -- Obtém os vizinhos do município 
    let Just (DA.Object municipioObj) = HM.lookup (T.pack municipioId) raiz
    let Just (DA.Array vizinhos) = HM.lookup (T.pack "nei") municipioObj

    let vizIds1 = DV.toList vizinhos

    let vizIds2 = fmap (\ (DA.Number n) -> (show n)) vizIds1

    -- Hack para tirar o ".0" da String
    let vizIds3 = map (trimNum "") vizIds2

    -- Retirar o proprio município
    let vizIds = filter (\id -> id /= municipioId) vizIds3

    exibirTabela municipioId nome tabela vizIds municipioIds

    gerarGraficos tabela mkNomeArqGrafico nome municipioId municipioIds vizIds pastaOutput

    mostrarGraficos mkNomeArqGrafico

    return ()    

-- Obtem o path do arquivo Json com a id do município e pasta de output
getMunicipioFile :: String -> FilePath -> FilePath
getMunicipioFile municipioId pastaOutput = pastaOutput ++ "/" ++ municipioId ++ ".json"

consultaIbgeId :: FilePath -> Map.Map Field Field -> FZ.FuzzySet -> DA.Object ->  Integer -> IO ()
consultaIbgeId pastaOutput municipioIds municipioFuzzy raiz entrada = do
    let ibgeId = show entrada
    putStrLn ("Procurando na tabela por entradas com IBGE ID=" ++ ibgeId)
    let municipioFile = getMunicipioFile ibgeId pastaOutput
    jsonExiste <- procuraArquivoMunicipio municipioFile
    
    if (jsonExiste) then do 
        carregarDadosMunicipio ibgeId pastaOutput (exibirMunicipio pastaOutput ibgeId municipioIds raiz)
    else do
        putStrLn municipioNaoEncontrado
    return ()

gerarSugestao municipioIds (i, j) = "(ID: " ++ (nomeParaIbgeId municipioIds k) ++ ") - " ++ k where
    k = T.unpack j

-- TODO: Melhorar performance de O(n) para O(1)
nomeParaIbgeId municipioIds nome = ibgeId where
    listaMunicipios = Map.toList municipioIds
    hasName i = (snd i) == nome
    listaEncontrado = filter hasName listaMunicipios
    ibgeId = fst (head listaEncontrado)

consultaNome pastaOutput municipioIds municipioFuzzy nome raiz _ = do
        putStrLn ("Pesquisando por " ++ nome ++ " em " ++ (show (FZ.size municipioFuzzy)) ++ " municípios")
        let resultados = take maxSugestoesNome (pesquisaMunicipio municipioFuzzy nome)

        if ( (length resultados) == 0) then do
            putStrLn "Nenhum município encontrado. Tente utilizar uma busca com palavras chaves mais abrangentes." 
        else do
            mapM_ putStrLn (map (gerarSugestao municipioIds) resultados)
            let primeiroNome = T.unpack (snd (head resultados))
            let ibgeId = nomeParaIbgeId municipioIds primeiroNome
            consultaIbgeId pastaOutput municipioIds municipioFuzzy raiz (read ibgeId :: Integer)

-- Recebe um município de entrada e se possível exibe suas informacoes 
receberInput :: FilePath -> Map.Map Field Field -> FZ.FuzzySet -> DA.Object -> IO ()
receberInput pastaOutput municipioIds municipioFuzzy raiz = do
    entrada <- perguntaCodIBGE

    -- Tenta converter para Int, se funcionar é Código IBGE
    
    let input = TR.readEither entrada :: Either String Integer

    let Right ibgeId = input

    either (consultaNome pastaOutput municipioIds municipioFuzzy entrada raiz) (consultaIbgeId pastaOutput municipioIds municipioFuzzy raiz) input

    putStrLn ""
    next <- receberInput pastaOutput municipioIds municipioFuzzy raiz
    return ()

-- Gera um FuzzySet com os nomes dos municícipios
municipiosSet :: [Field] -> FZ.FuzzySet
municipiosSet dados = FZ.fromList (map T.pack dados)

-- Pesquisa um municipio 
pesquisaMunicipio :: FZ.FuzzySet -> String -> [(Double, T.Text)]
pesquisaMunicipio municipiosSet input = FZ.get municipiosSet (T.pack input)

listaParaTupla [x, y] = (x, y)

indexarCodigosIBGE :: FilePath -> IO (Map.Map Field Field)
indexarCodigosIBGE arq = do
    conteudo <- carregarCSVMunicipioId arq
    csv <- parseCSVMunicipioId arq conteudo
    let Right resultado = csv 
    -- Remover o header
    let linhas = tail resultado
    let dados = filter (\l -> length l > 1) linhas
    putStrLn ("Filtradas " ++ (show (length dados)) ++ " de " ++ (show (length linhas)) ++ " linhas")
    let mapa = Map.fromList (map (listaParaTupla . take 2) dados)
    return mapa

carregarCSVMunicipioId :: FilePath -> IO String
carregarCSVMunicipioId arquivo = do
    putStrLn "Carregando o arquivo de municipios"
    conteudo <- readFile arquivo
    putStrLn "Arquivo Carregado"
    mostrarHorario
    return conteudo

parseCSVMunicipioId :: FilePath -> String -> IO (Either TErr.ParseError CSV)
parseCSVMunicipioId arquivo conteudo = do
    putStrLn "Realizando o parsing do CSV de municípios."
    let csv = parseCSV arquivo conteudo
    -- Verifica se foi bem sucedido
    mostrarHorario  
    return csv

-- Funcao principal
main :: IO ()
main = do
    putStrLn ""
    putStrLn ""
    putStrLn ""
    inicio <- getCurrentTime
    print inicio
    putStrLn "Fornecer o path para pasta de recursos"
    -- TODO: Nao pode ter / no final
    recursos <- getLine
    let arquivoCOVID = recursos ++ "/data/caso_full.csv"
    let arquivoMunicipioInfo = recursos ++ "/data/municipio-info.csv"
    let pastaOutput = recursos ++ "/output"

    existeTabelaMunicipiosFile <- doesFileExist arquivoMunicipioInfo
    if existeTabelaMunicipiosFile then do
        putStrLn "Arquivo de Informações dos Municípios encontrado"
        else do
            putStrLn "Arquivo de Informações dos Municípios NÃO encontrado"

            -- Baixa do GitHub   
            putStrLn "Baixando arquivo do repositório do GitHub"
            baixarDados urlMunicipioIdGithub arquivoMunicipioInfo
            putStrLn "Download Finalizado"

    -- Carrega os indices (ibgeID, nome do municipio)

    municipioIds <- indexarCodigosIBGE arquivoMunicipioInfo
    let listaIds = map snd (Map.toList municipioIds)
    let municipioFuzzy = municipiosSet listaIds
    putStrLn ("Carregamento dos índices Finalizado. " ++ (show (Map.size municipioIds)) ++ " municípios.")

    existeBrasilIoFile <- doesFileExist arquivoCOVID

    if existeBrasilIoFile then do
        putStrLn "Arquivo de dados do Brasil.IO encontrado"
        else do
            putStrLn "Arquivo de dados do Brasil.IO NÃO encontrado"

            -- Baixa as últimas estatísticas da COVID no site Brasil.IO    
            putStrLn "Baixando dados do Brasil.IO"
            baixarDados urlDataSet (arquivoCOVID ++ ".gz")
            putStrLn "Download Finalizado"

            putStrLn "Descompactando arquivo .gz"
            descompactar (arquivoCOVID ++ ".gz") arquivoCOVID
            putStrLn "Arquivo Descompactado"

            preIndexarDadosCovid pastaOutput arquivoCOVID
            putStrLn "Carregamento Finalizado"       

    -- Baixar arquivo vizinhos.json do Github
    let vizinhosFile = recursos ++ "/data/vizinhos.json"

    existeVizinhosFile <- doesFileExist vizinhosFile
    if existeVizinhosFile then do
        putStrLn "Arquivo de Informações dos Municípios Vizinhos encontrado"
        else do
            putStrLn "Arquivo de Informações dos Municípios Vizinhos NÃO encontrado"

            -- Baixa do GitHub   
            putStrLn "Baixando arquivo do repositório do GitHub"
            baixarDados urlMunicipiosVizinhosGithub vizinhosFile
            putStrLn "Download Finalizado"

    putStrLn ""

    -- Carrega o arquivo JSON de vizinhos
    resultadoVizinhos <- BS.readFile vizinhosFile
    let Just vizinhos = DA.decode resultadoVizinhos :: Maybe DA.Value
    let DA.Object raiz = vizinhos

    receberInput pastaOutput municipioIds municipioFuzzy raiz
    return ()