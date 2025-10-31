package br.com.rvneto.aws.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import net.coobird.thumbnailator.Thumbnails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public class ThumbnailHandler implements RequestHandler<S3Event, String> {
    
    private static final String TARGET_BUCKET = "rvneto-aws-s3-thumbnails";
    private static final int THUMBNAIL_SIZE = 150;
    
    // Criamos o cliente S3 fora do handler para reutilizá-lo (otimização)
    private final S3Client s3Client = S3Client.builder().build();
    
    @Override
    public String handleRequest(S3Event s3event, Context context) {
        LambdaLogger logger = context.getLogger();
        
        try {
            if (s3event.getRecords().isEmpty()) {
                logger.log("EVENTO S3 vazio, sem records.");
                return "Erro: Evento S3 vazio.";
            }
            
            S3EventNotificationRecord record = s3event.getRecords().get(0);
            String sourceBucket = record.getS3().getBucket().getName();
            
            // O nome do arquivo pode vir com codificação de URL (ex: "espaço" vira "+")
            String sourceKey = URLDecoder.decode(record.getS3().getObject().getKey(), StandardCharsets.UTF_8);
            
            logger.log("Evento recebido! Bucket: " + sourceBucket + ", Arquivo: " + sourceKey);
            
            // 1. Baixar a imagem original do S3
            ResponseInputStream<?> s3Object = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(sourceBucket)
                    .key(sourceKey)
                    .build());
            
            // 2. Criar a thumbnail em memória
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            Thumbnails.of(s3Object)
                    .size(THUMBNAIL_SIZE, THUMBNAIL_SIZE)
                    .outputFormat("jpg") // Converte para JPG
                    .toOutputStream(outputStream);
            
            // 3. Fazer upload da thumbnail para o bucket de destino
            String targetKey = "thumb-" + sourceKey; // Adiciona um prefixo
            
            s3Client.putObject(PutObjectRequest.builder()
                            .bucket(TARGET_BUCKET)
                            .key(targetKey)
                            .contentType("image/jpeg")
                            .build(),
                    RequestBody.fromInputStream(new ByteArrayInputStream(outputStream.toByteArray()), outputStream.size())
            );
            
            logger.log("Thumbnail criada com sucesso! Salva em: " + TARGET_BUCKET + "/" + targetKey);
            return "OK";
            
        } catch (Exception e) {
            logger.log("ERRO AO PROCESSAR IMAGEM: " + e.getMessage());
            e.printStackTrace();
            return "Erro";
        }
    }
}