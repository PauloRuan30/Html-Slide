package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Configurações
const (
	numProdutos    = 5000
	numLojas       = 50
	numPDVs        = 500
	numCaixas      = 500
	numClientes    = 25000
	numFornecedores = 10000
	numCidades     = 2000
	numNotasFiscais = 100000
	numItensNotaFiscal = 100000
	
	// Conexões para os bancos de dados
	mongoURI      = "mongodb://localhost:27017"
	cassandraHost = "127.0.0.1"
	
	// Nomes das databases
	mongoDB       = "varejo"
	cassandraKeyspace = "meu_keyspace"
	
	// Paralelismo para gerar dados mais rapidamente
	numGoroutines = 10
)

// Estruturas de dados
type Produto struct {
	CodProduto     int     `bson:"cod_produto"`
	NomProduto     string  `bson:"nom_produto"`
	CodFornecedor  int     `bson:"cod_fornecedor"`
	CodSetor       int     `bson:"cod_setor"`
	CodUnidade     int     `bson:"cod_unidade"`
	FlgFracionado  string  `bson:"flg_fracionado"`
	VlrVenda       float64 `bson:"vlr_venda"`
	VlrCusto       float64 `bson:"vlr_custo"`
	VlrMedio       float64 `bson:"vlr_medio"`
	CodPromocao    int     `bson:"cod_promocao,omitempty"`
	VlrPromocao    float64 `bson:"vlr_promocao,omitempty"`
}

type Loja struct {
	CodLoja      int    `bson:"cod_loja"`
	NomLoja      string `bson:"nom_loja"`
	CodEndereco  int    `bson:"cod_endereco"`
	FlgMatriz    string `bson:"flg_matriz"`
}

type PDV struct {
	CodPDV           int       `bson:"cod_pdv"`
	NumRegistro      float64   `bson:"num_registro"`
	DatInicioVigencia time.Time `bson:"dat_inicio_vigencia"`
	DatFimVigencia   time.Time `bson:"dat_fim_vigencia"`
	NumNotaInicial   float64   `bson:"num_nota_inicial"`
	NumNotaFinal     float64   `bson:"num_nota_final"`
	CodLoja          int       `bson:"cod_loja"`
	NumPDVLoja       float64   `bson:"num_pdv_loja"`
}

type Caixa struct {
	CodCaixa  int    `bson:"cod_caixa"`
	NomCaixa  string `bson:"nom_caixa"`
	CodLoja   int    `bson:"cod_loja"`
	FlgFerias string `bson:"flg_ferias"`
}

type Cidade struct {
	CodIBGE    int    `bson:"cod_ibge"`
	NomCidade  string `bson:"nom_cidade"`
	NomEstado  string `bson:"nom_estado"`
	NomRegiao  string `bson:"nom_regiao"`
	NomPais    string `bson:"nom_pais"`
}

type Endereco struct {
	CodEndereco    int    `bson:"cod_endereco"`
	NomLogradouro  string `bson:"nom_logradouro"`
	NumLogradouro  string `bson:"num_logradouro"`
	CodCEP         float64 `bson:"cod_cep"`
	CodIBGE        int    `bson:"cod_ibge"`
	FlgExterior    string `bson:"flg_exterior"`
	TipLogradouro  string `bson:"tip_logradouro"`
}

type Cliente struct {
	CodCliente    int    `bson:"cod_cliente"`
	NomCliente    string `bson:"nom_cliente"`
	FlgFidelizado string `bson:"flg_fidelizado"`
	CodEndereco   int    `bson:"cod_endereco"`
}

type Fornecedor struct {
	CodFornecedor  int    `bson:"cod_fornecedor"`
	NomFornecedor  string `bson:"nom_fornecedor"`
	FlgFatura      string `bson:"flg_fatura"`
	NumDiasFatura  float64 `bson:"num_dias_fatura"`
}

type NotaFiscal struct {
	SeqNota      int       `bson:"seq_nota"`
	CodPDV       int       `bson:"cod_pdv"`
	CodCaixa     int       `bson:"cod_caixa"`
	CodCliente   int       `bson:"cod_cliente"`
	NumNota      float64   `bson:"num_nota"`
	DatNota      time.Time `bson:"dat_nota"`
	FlgEntrega   string    `bson:"flg_entrega"`
	VlrNota      float64   `bson:"vlr_nota"`
	VlrDinheiro  float64   `bson:"vlr_dinheiro"`
	VlrTick      float64   `bson:"vlr_tick"`
	VlrCartao    float64   `bson:"vlr_cartao"`
}

type ItemNotaFiscal struct {
	SeqItemNota int     `bson:"seq_item_nota"`
	SeqNota     int     `bson:"seq_nota"`
	CodProduto  int     `bson:"cod_produto"`
	QtdProduto  float64 `bson:"qtd_produto"`
	VlrVenda    float64 `bson:"vlr_venda"`
	VlrCusto    float64 `bson:"vlr_custo"`
	VlrMedio    float64 `bson:"vlr_medio"`
	VlrPromocao float64 `bson:"vlr_promocao"`
}

// Variáveis globais
var (
	estados = []string{
		"AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA",
		"MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN",
		"RS", "RO", "RR", "SC", "SP", "SE", "TO",
	}
	
	regioes = []string{
		"Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul",
	}
	
	tiposLogradouro = []string{
		"R", "AV", "AL", "EST", "ROD", "PRÇ", "VL",
	}
	
	setores = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	unidades = []int{1, 2, 3, 4, 5}
	
	nomesProdutos = []string{
		"Arroz", "Feijão", "Macarrão", "Açúcar", "Café", "Leite", "Óleo",
		"Farinha", "Sal", "Carne", "Frango", "Peixe", "Pão", "Cerveja",
		"Refrigerante", "Suco", "Biscoito", "Chocolate", "Sorvete", "Sabão",
		"Detergente", "Desinfetante", "Papel Higiênico", "Shampoo", "Condicionador",
	}
	
	sobrenomesProdutos = []string{
		"Tipo 1", "Premium", "Gold", "Silver", "Tradicional", "Especial",
		"Extra", "Super", "Master", "Light", "Integral", "Natural",
		"Original", "Fino", "Clássico", "Orgânico", "Zero", "Plus",
		"Mega", "Ultra", "Soft", "Fresh", "Tropical", "Gourmet",
	}
	
	marcasProdutos = []string{
		"Nova Era", "Tradição", "Qualidade", "Campo Bom", "Delícia", 
		"Saúde Total", "Sabor Perfeito", "MasterFood", "Naturalmente", 
		"BomGosto", "AmigoDia", "CasaFeliz", "PuroBem", "DeliciaReal",
	}
	
	sobrenomesPessoas = []string{
		"Silva", "Santos", "Oliveira", "Souza", "Lima", "Pereira", "Ferreira",
		"Costa", "Rodrigues", "Almeida", "Nascimento", "Carvalho", "Gomes",
		"Martins", "Araújo", "Ribeiro", "Monteiro", "Cardoso", "Correia",
	}
	
	nomesPessoas = []string{
		"João", "Maria", "José", "Ana", "Pedro", "Paulo", "Carlos", "Marcos",
		"Lucas", "Mateus", "Gabriel", "Rafael", "Daniel", "Antônio", "Fernando",
		"Luiz", "Eduardo", "André", "Adriana", "Amanda", "Bruna", "Camila",
		"Carolina", "Cláudia", "Débora", "Diana", "Eliana", "Fernanda", "Gabriela",
	}
	
	nomesLogradouros = []string{
		"Flores", "Palmeiras", "Ipê", "Jatobá", "Araçá", "Tucumã", "Brasil",
		"Santos Dumont", "Getúlio Vargas", "JK", "Amazonas", "Rui Barbosa",
		"Marechal Deodoro", "Principal", "Comercial", "Industrial", "Central",
		"Jatoba", "das Araras", "dos Bandeirantes", "Coronel Fawcett",
	}
)

func main() {
	rand.Seed(time.Now().UnixNano())
	
	// Cria conexões com os bancos de dados
	mongoClient, err := conectarMongoDB()
	if err != nil {
		log.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(context.Background())
	
	cassandraSession, err := conectarCassandra()
	if err != nil {
		log.Fatalf("Erro ao conectar ao Cassandra: %v", err)
	}
	defer cassandraSession.Close()
	
	// Contexto para operações do MongoDB
	ctx := context.Background()
	
	// Inicia contadores de progresso
	fmt.Println("Iniciando geração de dados...")
	
	// Gera dados para Cidades
	fmt.Println("Gerando cidades...")
	gerarCidades(ctx, mongoClient, cassandraSession)
	
	// Gera dados para Endereços
	fmt.Println("Gerando endereços...")
	gerarEnderecos(ctx, mongoClient, cassandraSession)
	
	// Gera dados para Fornecedores
	fmt.Println("Gerando fornecedores...")
	gerarFornecedores(ctx, mongoClient, cassandraSession)
	
	// Gera dados para Produtos
	fmt.Println("Gerando produtos...")
	gerarProdutos(ctx, mongoClient, cassandraSession)
	
	// Gera dados para Lojas
	fmt.Println("Gerando lojas...")
	gerarLojas(ctx, mongoClient, cassandraSession)
	
	// Gera dados para PDVs
	fmt.Println("Gerando PDVs...")
	gerarPDVs(ctx, mongoClient, cassandraSession)
	
	// Gera dados para Caixas
	fmt.Println("Gerando caixas...")
	gerarCaixas(ctx, mongoClient, cassandraSession)
	
	// Gera dados para Clientes
	fmt.Println("Gerando clientes...")
	gerarClientes(ctx, mongoClient, cassandraSession)
	
	// Gera dados para Notas Fiscais e Itens
	fmt.Println("Gerando notas fiscais e itens...")
	gerarNotasFiscaisEItens(ctx, mongoClient, cassandraSession)
	
	fmt.Println("Geração de dados concluída com sucesso!")
}

// Funções de conexão com bancos de dados
func conectarMongoDB() (*mongo.Client, error) {
	clientOptions := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, err
	}
	
	err = client.Ping(context.Background(), nil)
	if err != nil {
		return nil, err
	}
	
	fmt.Println("Conectado ao MongoDB com sucesso!")
	return client, nil
}

func conectarCassandra() (*gocql.Session, error) {
	cluster := gocql.NewCluster(cassandraHost)
	cluster.Keyspace = cassandraKeyspace
	cluster.Consistency = gocql.Quorum
	
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	
	fmt.Println("Conectado ao Cassandra com sucesso!")
	return session, nil
}

// Funções geradoras de dados
func gerarCidades(ctx context.Context, mongoClient *mongo.Client, cassandraSession *gocql.Session) {
	collection := mongoClient.Database(mongoDB).Collection("cidade")
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	itemsPerGoroutine := numCidades / numGoroutines
	
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			start := goroutineID * itemsPerGoroutine
			end := start + itemsPerGoroutine
			if goroutineID == numGoroutines-1 {
				end = numCidades
			}
			
			for i := start; i < end; i++ {
				codIBGE := i + 1000000
				estado := estados[rand.Intn(len(estados))]
				regiao := regioes[rand.Intn(len(regioes))]
				nomCidade := fmt.Sprintf("Cidade %d", i+1)
				
				cidade := Cidade{
					CodIBGE:   codIBGE,
					NomCidade: nomCidade,
					NomEstado: estado,
					NomRegiao: regiao,
					NomPais:   "Brasil",
				}
				
				// Insere no MongoDB
				_, err := collection.InsertOne(ctx, cidade)
				if err != nil {
					log.Printf("Erro ao inserir cidade no MongoDB: %v", err)
				}
				
				// Insere no Cassandra
				err = cassandraSession.Query(`
					INSERT INTO cidade (cod_ibge, nom_cidade, nom_estado, nom_regiao, nom_pais)
					VALUES (?, ?, ?, ?, ?)
				`, cidade.CodIBGE, cidade.NomCidade, cidade.NomEstado, cidade.NomRegiao, cidade.NomPais).Exec()
				
				if err != nil {
					log.Printf("Erro ao inserir cidade no Cassandra: %v", err)
				}
			}
		}(g)
	}
	
	wg.Wait()
	fmt.Printf("Geradas %d cidades\n", numCidades)
}

func gerarEnderecos(ctx context.Context, mongoClient *mongo.Client, cassandraSession *gocql.Session) {
	collection := mongoClient.Database(mongoDB).Collection("endereco")
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	// Precisamos de pelo menos tantos endereços quanto clientes + lojas
	totalEnderecos := numClientes + numLojas
	itemsPerGoroutine := totalEnderecos / numGoroutines
	
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			start := goroutineID * itemsPerGoroutine
			end := start + itemsPerGoroutine
			if goroutineID == numGoroutines-1 {
				end = totalEnderecos
			}
			
			for i := start; i < end; i++ {
				codEndereco := i + 1
				nomLogradouro := nomesLogradouros[rand.Intn(len(nomesLogradouros))]
				numLogradouro := fmt.Sprintf("%d", rand.Intn(1000)+1)
				codCEP := float64(10000000 + rand.Intn(90000000))
				codIBGE := rand.Intn(numCidades) + 1000000
				flgExterior := "N"
				tipLogradouro := tiposLogradouro[rand.Intn(len(tiposLogradouro))]
				
				endereco := Endereco{
					CodEndereco:   codEndereco,
					NomLogradouro: nomLogradouro,
					NumLogradouro: numLogradouro,
					CodCEP:        codCEP,
					CodIBGE:       codIBGE,
					FlgExterior:   flgExterior,
					TipLogradouro: tipLogradouro,
				}
				
				// Insere no MongoDB
				_, err := collection.InsertOne(ctx, endereco)
				if err != nil {
					log.Printf("Erro ao inserir endereço no MongoDB: %v", err)
				}
				
				// Insere no Cassandra
				err = cassandraSession.Query(`
					INSERT INTO endereco (cod_endereco, nom_logradouro, num_logradouro, cod_cep, cod_ibge, flg_exterior, tip_logradouro)
					VALUES (?, ?, ?, ?, ?, ?, ?)
				`, endereco.CodEndereco, endereco.NomLogradouro, endereco.NumLogradouro, 
				endereco.CodCEP, endereco.CodIBGE, endereco.FlgExterior, endereco.TipLogradouro).Exec()
				
				if err != nil {
					log.Printf("Erro ao inserir endereço no Cassandra: %v", err)
				}
			}
		}(g)
	}
	
	wg.Wait()
	fmt.Printf("Gerados %d endereços\n", totalEnderecos)
}

func gerarFornecedores(ctx context.Context, mongoClient *mongo.Client, cassandraSession *gocql.Session) {
	collection := mongoClient.Database(mongoDB).Collection("fornecedor")
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	itemsPerGoroutine := numFornecedores / numGoroutines
	
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			start := goroutineID * itemsPerGoroutine
			end := start + itemsPerGoroutine
			if goroutineID == numGoroutines-1 {
				end = numFornecedores
			}
			
			for i := start; i < end; i++ {
				codFornecedor := i + 1
				nome1 := nomesPessoas[rand.Intn(len(nomesPessoas))]
				nome2 := sobrenomesPessoas[rand.Intn(len(sobrenomesPessoas))]
				nomFornecedor := fmt.Sprintf("%s %s Ltda", nome1, nome2)
				flgFatura := ""
				numDiasFatura := float64(0)
				
				if rand.Intn(2) == 1 {
					flgFatura = "S"
					numDiasFatura = float64(rand.Intn(30) + 1)
				} else {
					flgFatura = "N"
				}
				
				fornecedor := Fornecedor{
					CodFornecedor: codFornecedor,
					NomFornecedor: nomFornecedor,
					FlgFatura:     flgFatura,
					NumDiasFatura: numDiasFatura,
				}
				
				// Insere no MongoDB
				_, err := collection.InsertOne(ctx, fornecedor)
				if err != nil {
					log.Printf("Erro ao inserir fornecedor no MongoDB: %v", err)
				}
				
				// Insere no Cassandra
				err = cassandraSession.Query(`
					INSERT INTO fornecedor (cod_fornecedor, nom_fornecedor, flg_fatura, num_dias_fatura)
					VALUES (?, ?, ?, ?)
				`, fornecedor.CodFornecedor, fornecedor.NomFornecedor, fornecedor.FlgFatura, fornecedor.NumDiasFatura).Exec()
				
				if err != nil {
					log.Printf("Erro ao inserir fornecedor no Cassandra: %v", err)
				}
			}
		}(g)
	}
	
	wg.Wait()
	fmt.Printf("Gerados %d fornecedores\n", numFornecedores)
}

func gerarProdutos(ctx context.Context, mongoClient *mongo.Client, cassandraSession *gocql.Session) {
	collection := mongoClient.Database(mongoDB).Collection("produto")
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	itemsPerGoroutine := numProdutos / numGoroutines
	
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			start := goroutineID * itemsPerGoroutine
			end := start + itemsPerGoroutine
			if goroutineID == numGoroutines-1 {
				end = numProdutos
			}
			
			for i := start; i < end; i++ {
				codProduto := i + 1
				
				// Gera um nome de produto combinando elementos
				nomeProduto := fmt.Sprintf("%s %s %s", 
					marcasProdutos[rand.Intn(len(marcasProdutos))],
					nomesProdutos[rand.Intn(len(nomesProdutos))],
					sobrenomesProdutos[rand.Intn(len(sobrenomesProdutos))],
				)
				
				codFornecedor := rand.Intn(numFornecedores) + 1
				codSetor := setores[rand.Intn(len(setores))]
				codUnidade := unidades[rand.Intn(len(unidades))]
				
				// Preços
				vlrCusto := 5.0 + rand.Float64()*95.0 // De 5 a 100
				vlrCusto = float64(int(vlrCusto*100)) / 100 // Arredonda para 2 casas decimais
				
				margem := 1.2 + rand.Float64()*0.8 // Margem de 20% a 100%
				vlrVenda := vlrCusto * margem
				vlrVenda = float64(int(vlrVenda*100)) / 100 // Arredonda para 2 casas decimais
				
				vlrMedio := (vlrCusto + vlrVenda) / 2
				vlrMedio = float64(int(vlrMedio*100)) / 100 // Arredonda para 2 casas decimais
				
				// Flags e valores opcionais
				flgFracionado := "N"
				if rand.Intn(10) < 3 { // 30% dos produtos são fracionados
					flgFracionado = "S"
				}
				
				var codPromocao *int
				var vlrPromocao *float64
				
				// 20% dos produtos estão em promoção
				if rand.Intn(10) < 2 {
					promo := rand.Intn(20) + 1
					codPromocao = &promo
					
					promoVal := vlrVenda * 0.7 // 30% de desconto
					promoVal = float64(int(promoVal*100)) / 100 // Arredonda para 2 casas decimais
					vlrPromocao = &promoVal
				}
				
				// Monta o objeto produto
				produto := bson.M{
					"cod_produto":    codProduto,
					"nom_produto":    nomeProduto,
					"cod_fornecedor": codFornecedor,
					"cod_setor":      codSetor,
					"cod_unidade":    codUnidade,
					"flg_fracionado": flgFracionado,
					"vlr_venda":      vlrVenda,
					"vlr_custo":      vlrCusto,
					"vlr_medio":      vlrMedio,
				}
				
				if codPromocao != nil {
					produto["cod_promocao"] = *codPromocao
					produto["vlr_promocao"] = *vlrPromocao
				}
				
				// Insere no MongoDB
				_, err := collection.InsertOne(ctx, produto)
				if err != nil {
					log.Printf("Erro ao inserir produto no MongoDB: %v", err)
				}
				
				// Insere no Cassandra
				// Devido ao esquema do Cassandra, temos que lidar com nulos de outra forma
				var promocaoCod int
				var promocaoVlr float64
				
				if codPromocao != nil {
					promocaoCod = *codPromocao
					promocaoVlr = *vlrPromocao
				}
				
				err = cassandraSession.Query(`
					INSERT INTO produto (cod_produto, nom_produto, cod_fornecedor, cod_setor, cod_unidade, 
					                     flg_fracionado, vlr_venda, vlr_custo, vlr_medio, cod_promocao, vlr_promocao)
					VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
				`, codProduto, nomeProduto, codFornecedor, codSetor, codUnidade, 
				flgFracionado, vlrVenda, vlrCusto, vlrMedio, promocaoCod, promocaoVlr).Exec()
				
				if err != nil {
					log.Printf("Erro ao inserir produto no Cassandra: %v", err)
				}
			}
		}(g)
	}
	
	wg.Wait()
	fmt.Printf("Gerados %d produtos\n", numProdutos)
}

func gerarLojas(ctx context.Context, mongoClient *mongo.Client, cassandraSession *gocql.Session) {
	collection := mongoClient.Database(mongoDB).Collection("loja")
	
	for i := 0; i < numLojas; i++ {
		codLoja := i + 1
		nomLoja := fmt.Sprintf("Loja %d", codLoja)
		
		// Usa os primeiros endereços para as lojas
		codEndereco := i + 1
		
		flgMatriz := "N"
		if i == 0 {
			flgMatriz = "S"
		}
		
		loja := Loja{
			CodLoja:     codLoja,
			NomLoja:     nomLoja,
			CodEndereco: codEndereco,
			FlgMatriz:   flgMatriz,
		}
		
		// Insere no MongoDB
		_, err := collection.InsertOne(ctx, loja)
		if err != nil {
			log.Printf("Erro ao inserir loja no MongoDB: %v", err)
		}
		
		// Insere no Cassandra
		err = cassandraSession.Query(`
			INSERT INTO loja (cod_loja, nom_loja, cod_endereco, flg_matriz)
			VALUES (?, ?, ?, ?)
		`, loja.CodLoja, loja.NomLoja, loja.CodEndereco, loja.FlgMatriz).Exec()
		
		if err != nil {
			log.Printf("Erro ao inserir loja no Cassandra: %v", err)
		}
	}
	
	fmt.Printf("Geradas %d lojas\n", numLojas)
}

func gerarPDVs(ctx context.Context, mongoClient *mongo.Client, cassandraSession *gocql.Session) {
	collection := mongoClient.Database(mongoDB).Collection("pdv")
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	itemsPerGoroutine := numPDVs / numGoroutines
	
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			start := goroutineID * itemsPerGoroutine
			end := start + itemsPerGoroutine
			if goroutineID == numGoroutines-1 {
				end = numPDVs
			}
			
			for i := start; i < end; i++ {
				codPDV := i + 1
				numRegistro := float64(rand.Intn(9000) + 1000)
				
                // Datas de vigência
                agora := time.Now()
                dataInicio := agora.AddDate(-1, -rand.Intn(12), -rand.Intn(30))
                dataFim := dataInicio.AddDate(5, 0, 0) // Vigência de 5 anos
                
                // Notas fiscais
                numNotaInicial := float64(rand.Intn(1000) + 1)
                numNotaFinal := numNotaInicial + float64(rand.Intn(9000) + 1000)
                
                // Loja associada ao PDV
                codLoja := rand.Intn(numLojas) + 1
                numPDVLoja := float64(rand.Intn(20) + 1) // Número do PDV dentro da loja
                
                pdv := PDV{
                    CodPDV:           codPDV,
                    NumRegistro:      numRegistro,
                    DatInicioVigencia: dataInicio,
                    DatFimVigencia:   dataFim,
                    NumNotaInicial:   numNotaInicial,
                    NumNotaFinal:     numNotaFinal,
                    CodLoja:          codLoja,
                    NumPDVLoja:       numPDVLoja,
                }
                
                // Insere no MongoDB
                _, err := collection.InsertOne(ctx, pdv)
                if err != nil {
                    log.Printf("Erro ao inserir PDV no MongoDB: %v", err)
                }
                
                // Insere no Cassandra
                err = cassandraSession.Query(`
                    INSERT INTO pdv (cod_pdv, num_registro, dat_inicio_vigencia, dat_fim_vigencia, 
                                   num_nota_inicial, num_nota_final, cod_loja, num_pdv_loja)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                `, pdv.CodPDV, pdv.NumRegistro, pdv.DatInicioVigencia, pdv.DatFimVigencia,
                pdv.NumNotaInicial, pdv.NumNotaFinal, pdv.CodLoja, pdv.NumPDVLoja).Exec()
                
                if err != nil {
                    log.Printf("Erro ao inserir PDV no Cassandra: %v", err)
                }
            }
        }(g)
    }
    
    wg.Wait()
    fmt.Printf("Gerados %d PDVs\n", numPDVs)
}

func gerarCaixas(ctx context.Context, mongoClient *mongo.Client, cassandraSession *gocql.Session) {
    collection := mongoClient.Database(mongoDB).Collection("caixa")
    
    var wg sync.WaitGroup
    wg.Add(numGoroutines)
    itemsPerGoroutine := numCaixas / numGoroutines
    
    for g := 0; g < numGoroutines; g++ {
        go func(goroutineID int) {
            defer wg.Done()
            
            start := goroutineID * itemsPerGoroutine
            end := start + itemsPerGoroutine
            if goroutineID == numGoroutines-1 {
                end = numCaixas
            }
            
            for i := start; i < end; i++ {
                codCaixa := i + 1
                
                // Nome do operador de caixa
                nome := nomesPessoas[rand.Intn(len(nomesPessoas))]
                sobrenome := sobrenomesPessoas[rand.Intn(len(sobrenomesPessoas))]
                nomCaixa := fmt.Sprintf("%s %s", nome, sobrenome)
                
                // Loja associada ao caixa
                codLoja := rand.Intn(numLojas) + 1
                
                // Situação de férias
                flgFerias := "N"
                if rand.Intn(10) < 1 { // 10% dos caixas estão de férias
                    flgFerias = "S"
                }
                
                caixa := Caixa{
                    CodCaixa: codCaixa,
                    NomCaixa: nomCaixa,
                    CodLoja:  codLoja,
                    FlgFerias: flgFerias,
                }
                
                // Insere no MongoDB
                _, err := collection.InsertOne(ctx, caixa)
                if err != nil {
                    log.Printf("Erro ao inserir caixa no MongoDB: %v", err)
                }
                
                // Insere no Cassandra
                err = cassandraSession.Query(`
                    INSERT INTO caixa (cod_caixa, nom_caixa, cod_loja, flg_ferias)
                    VALUES (?, ?, ?, ?)
                `, caixa.CodCaixa, caixa.NomCaixa, caixa.CodLoja, caixa.FlgFerias).Exec()
                
                if err != nil {
                    log.Printf("Erro ao inserir caixa no Cassandra: %v", err)
                }
            }
        }(g)
    }
    
    wg.Wait()
    fmt.Printf("Gerados %d caixas\n", numCaixas)
}

func gerarClientes(ctx context.Context, mongoClient *mongo.Client, cassandraSession *gocql.Session) {
    collection := mongoClient.Database(mongoDB).Collection("cliente")
    
    var wg sync.WaitGroup
    wg.Add(numGoroutines)
    itemsPerGoroutine := numClientes / numGoroutines
    
    for g := 0; g < numGoroutines; g++ {
        go func(goroutineID int) {
            defer wg.Done()
            
            start := goroutineID * itemsPerGoroutine
            end := start + itemsPerGoroutine
            if goroutineID == numGoroutines-1 {
                end = numClientes
            }
            
            for i := start; i < end; i++ {
                codCliente := i + 1
                
                // Nome do cliente
                nome := nomesPessoas[rand.Intn(len(nomesPessoas))]
                sobrenome := sobrenomesPessoas[rand.Intn(len(sobrenomesPessoas))]
                nomCliente := fmt.Sprintf("%s %s", nome, sobrenome)
                
                // Status de fidelização
                flgFidelizado := "N"
                if rand.Intn(10) < 4 { // 40% dos clientes são fidelizados
                    flgFidelizado = "S"
                }
                
                // Endereço do cliente (após os endereços das lojas)
                codEndereco := numLojas + i + 1
                
                cliente := Cliente{
                    CodCliente:    codCliente,
                    NomCliente:    nomCliente,
                    FlgFidelizado: flgFidelizado,
                    CodEndereco:   codEndereco,
                }
                
                // Insere no MongoDB
                _, err := collection.InsertOne(ctx, cliente)
                if err != nil {
                    log.Printf("Erro ao inserir cliente no MongoDB: %v", err)
                }
                
                // Insere no Cassandra
                err = cassandraSession.Query(`
                    INSERT INTO cliente (cod_cliente, nom_cliente, flg_fidelizado, cod_endereco)
                    VALUES (?, ?, ?, ?)
                `, cliente.CodCliente, cliente.NomCliente, cliente.FlgFidelizado, cliente.CodEndereco).Exec()
                
                if err != nil {
                    log.Printf("Erro ao inserir cliente no Cassandra: %v", err)
                }
            }
        }(g)
    }
    
    wg.Wait()
    fmt.Printf("Gerados %d clientes\n", numClientes)
}

func gerarNotasFiscaisEItens(ctx context.Context, mongoClient *mongo.Client, cassandraSession *gocql.Session) {
    colecaoNotas := mongoClient.Database(mongoDB).Collection("nota_fiscal")
    colecaoItens := mongoClient.Database(mongoDB).Collection("item_nota_fiscal")
    
    var wg sync.WaitGroup
    wg.Add(numGoroutines)
    itemsPerGoroutine := numNotasFiscais / numGoroutines
    
    // Precisamos de produtos pré-carregados para associar às notas
    produtos := make([]Produto, 0, numProdutos)
    cursor, err := mongoClient.Database(mongoDB).Collection("produto").Find(ctx, bson.M{})
    if err != nil {
        log.Printf("Erro ao buscar produtos: %v", err)
        return
    }
    
    if err = cursor.All(ctx, &produtos); err != nil {
        log.Printf("Erro ao decodificar produtos: %v", err)
        return
    }
    
    for g := 0; g < numGoroutines; g++ {
        go func(goroutineID int) {
            defer wg.Done()
            
            start := goroutineID * itemsPerGoroutine
            end := start + itemsPerGoroutine
            if goroutineID == numGoroutines-1 {
                end = numNotasFiscais
            }
            
            for i := start; i < end; i++ {
                seqNota := i + 1
                
                // Associações aleatórias
                codPDV := rand.Intn(numPDVs) + 1
                codCaixa := rand.Intn(numCaixas) + 1
                codCliente := rand.Intn(numClientes) + 1
                
                // Dados da nota
                numNota := float64(100000 + rand.Intn(900000))
                datNota := time.Now().AddDate(0, -rand.Intn(12), -rand.Intn(30))
                
                flgEntrega := "N"
                if rand.Intn(10) < 2 { // 20% com entrega
                    flgEntrega = "S"
                }
                
                // Cria a nota
                notaFiscal := NotaFiscal{
                    SeqNota:     seqNota,
                    CodPDV:      codPDV,
                    CodCaixa:    codCaixa,
                    CodCliente:  codCliente,
                    NumNota:     numNota,
                    DatNota:     datNota,
                    FlgEntrega:  flgEntrega,
                    VlrNota:     0, // Será calculado com base nos itens
                    VlrDinheiro: 0,
                    VlrTick:     0,
                    VlrCartao:   0,
                }
                
                // Gera itens para a nota (entre 1 e 15 itens por nota)
                numItens := rand.Intn(15) + 1
                itensNota := make([]ItemNotaFiscal, 0, numItens)
                
                for j := 0; j < numItens; j++ {
                    // Seleciona um produto aleatório
                    produtoIdx := rand.Intn(len(produtos))
                    produto := produtos[produtoIdx]
                    
                    // Quantidade vendida (entre 1 e 10, com decimais para produtos fracionados)
                    qtdProduto := float64(rand.Intn(10) + 1)
                    if produto.FlgFracionado == "S" {
                        // Adiciona fração para produtos fracionados
                        qtdProduto += float64(rand.Intn(10)) / 10
                    }
                    
                    // Valor de venda (usa o de promoção se existir)
                    vlrVenda := produto.VlrVenda
                    if produto.VlrPromocao > 0 {
                        vlrVenda = produto.VlrPromocao
                    }
                    
                    item := ItemNotaFiscal{
                        SeqItemNota: j + 1,
                        SeqNota:     seqNota,
                        CodProduto:  produto.CodProduto,
                        QtdProduto:  qtdProduto,
                        VlrVenda:    vlrVenda,
                        VlrCusto:    produto.VlrCusto,
                        VlrMedio:    produto.VlrMedio,
                        VlrPromocao: produto.VlrPromocao,
                    }
                    
                    itensNota = append(itensNota, item)
                    
                    // Acumula valor da nota
                    notaFiscal.VlrNota += vlrVenda * qtdProduto
                }
                
                // Arredonda o valor total
                notaFiscal.VlrNota = float64(int(notaFiscal.VlrNota*100)) / 100
                
                // Distribui o pagamento entre as formas
                formaPgto := rand.Intn(3)
                switch formaPgto {
                case 0: // Dinheiro
                    notaFiscal.VlrDinheiro = notaFiscal.VlrNota
                case 1: // Ticket
                    notaFiscal.VlrTick = notaFiscal.VlrNota
                case 2: // Cartão
                    notaFiscal.VlrCartao = notaFiscal.VlrNota
                }
                
                // Insere a nota no MongoDB
                _, err := colecaoNotas.InsertOne(ctx, notaFiscal)
                if err != nil {
                    log.Printf("Erro ao inserir nota fiscal no MongoDB: %v", err)
                    continue
                }
                
                // Insere a nota no Cassandra
                err = cassandraSession.Query(`
                    INSERT INTO nota_fiscal (seq_nota, cod_pdv, cod_caixa, cod_cliente, num_nota, 
                                           dat_nota, flg_entrega, vlr_nota, vlr_dinheiro, vlr_tick, vlr_cartao)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                `, notaFiscal.SeqNota, notaFiscal.CodPDV, notaFiscal.CodCaixa, notaFiscal.CodCliente, 
                notaFiscal.NumNota, notaFiscal.DatNota, notaFiscal.FlgEntrega, notaFiscal.VlrNota, 
                notaFiscal.VlrDinheiro, notaFiscal.VlrTick, notaFiscal.VlrCartao).Exec()
                
                if err != nil {
                    log.Printf("Erro ao inserir nota fiscal no Cassandra: %v", err)
                }
                
                // Insere os itens da nota
                for _, item := range itensNota {
                    // MongoDB
                    _, err := colecaoItens.InsertOne(ctx, item)
                    if err != nil {
                        log.Printf("Erro ao inserir item de nota fiscal no MongoDB: %v", err)
                    }
                    
                    // Cassandra
                    err = cassandraSession.Query(`
                        INSERT INTO item_nota_fiscal (seq_item_nota, seq_nota, cod_produto, qtd_produto, 
                                                    vlr_venda, vlr_custo, vlr_medio, vlr_promocao)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    `, item.SeqItemNota, item.SeqNota, item.CodProduto, item.QtdProduto,
                    item.VlrVenda, item.VlrCusto, item.VlrMedio, item.VlrPromocao).Exec()
                    
                    if err != nil {
                        log.Printf("Erro ao inserir item de nota fiscal no Cassandra: %v", err)
                    }
                }
                
                // Feedback de progresso a cada 1000 notas
                if i%1000 == 0 && i > 0 {
                    fmt.Printf("Geradas %d notas fiscais...\n", i)
                }
            }
        }(g)
    }
    
    wg.Wait()
    fmt.Printf("Geradas %d notas fiscais com itens\n", numNotasFiscais)
}
                                    
