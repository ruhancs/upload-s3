package main

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/joho/godotenv"
)

var (
	s3Client *s3.S3
	s3Bucket string
	wg       sync.WaitGroup
)

func init() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}
	//criar sessao na aws, com variaveis e credenciais
	sess, err := session.NewSession(
		&aws.Config{
			Region: aws.String(os.Getenv("REGION")),
			Credentials: credentials.NewStaticCredentials(
				os.Getenv("PK"),                     //credencial PK
				os.Getenv("SK"), //senha SK
				"", //token
			),
		},
	)
	if err != nil {
		panic(err)
	}
	s3Client = s3.New(sess)
	s3Bucket = os.Getenv("BUCKET_NAME")
}

func main() {
	//ler diretorio dos arquivos para upload
	dir, err := os.Open("./tmp")
	if err != nil {
		panic(err)
	}
	defer dir.Close()

	//channel para controlar o numero de threads criadas
	uploadControl := make(chan struct{}, 300)
	
	//verificar erros de upload
	errorFileUpload := make(chan string, 10)
	go func() {
		for {
			select{
			case filename := <- errorFileUpload:
				//refazer upload do arquivo
				uploadControl <- struct{}{}
				wg.Add(1)
				go uploadFile(filename,uploadControl, errorFileUpload)
			}
		}
	}()

	//loop para ler o diretorio continuamente e enviar os arquivos
	for {
		//ler conteudo da pasta, e retornar slice dos arquivos
		files, err := dir.ReadDir(1)
		if err != nil {
			//verficar se todos arquivos ja foram lidos
			if err == io.EOF {
				break //parar de ler os arquivos
			}
			//tenta ler novamente
			fmt.Println("error reading directory")
			continue
		}
		wg.Add(1)
		//inserir struct vazia para contar o numero de threads
		//quando channel tiver cheio nao deixara criar uma nova routine ate liberar espaco no channel
		uploadControl <- struct{}{}
		//sobe um arquivo de cada vez
		go uploadFile(files[0].Name(),uploadControl, errorFileUpload)
	}
	wg.Wait()
}

func uploadFile(filename string, uploadControl chan struct{}, errorChan chan string) {
	defer wg.Done()
	//caminho completo do arquivo
	completeFileName := fmt.Sprintf("./tmp/%s", filename)
	fmt.Printf("init upload file %s to bucket %s\n", completeFileName, s3Bucket)
	//abrir o arquivo
	f, err := os.Open(completeFileName)
	if err != nil {
		fmt.Printf("Error opening file: %s\n", completeFileName)
		//liberar espaco no channel
		<-uploadControl
		//inserir caminho do arquivo que deu erro no canal de erros
		errorChan <- completeFileName
		return
	}
	defer f.Close()

	//upload para aws
	_, err = s3Client.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(s3Bucket), //nome da bucket
		Key:    aws.String(filename), //nome do arquivo
		Body:   f,                    //conteudo do arquivo
	})
	if err != nil {
		fmt.Printf("Error uploading file: %s\n", completeFileName)
		//liberar espaco no channel
		<-uploadControl
		//inserir caminho do arquivo que deu erro no canal de erros
		errorChan <- completeFileName
		return
	}

	fmt.Printf("file %s uploaded to aws successfuly\n", completeFileName)
	//liberar espaco no channel
	<-uploadControl
}
