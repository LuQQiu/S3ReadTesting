package alluxio;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileDownload;
import software.amazon.awssdk.transfer.s3.model.DownloadFileRequest;
import software.amazon.awssdk.transfer.s3.model.FileDownload;

import java.io.File;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class Main {

  public static void main(String[] args) throws Exception {
    transferManager();
    getObjectWhole();
    readAsBytesWholeObject();
    System.exit(0);
  }

  public static void transferManager() throws Exception {
    String key = "alluxio-2.8.1-bin.tar.gz";
    String bucket = "lu-asf-demo";
    S3AsyncClient s3AsyncClient =
        S3AsyncClient.crtBuilder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .targetThroughputInGbps(20.0)
            .minimumPartSizeInBytes(8 * 1024L * 1024)
            .build();
    // 83MB/s default one without executor
    // 20 threads only 97MB/s, 460
    GetObjectRequest objectRequest = GetObjectRequest
        .builder()
        .key(key)
        .bucket(bucket)
        .build();
    File file = new File(key);
    file.delete();
    long start= System.currentTimeMillis()
    CompletableFuture<GetObjectResponse> futureGet = s3AsyncClient.getObject(objectRequest,
        AsyncResponseTransformer.toFile(file));
    futureGet.whenComplete((resp, err) -> {
      try {
        if (resp != null) {
          System.out.println("Object downloaded. Details: " + resp);
        } else {
          err.printStackTrace();
        }
      } finally {
        // Only close the client when you are completely done with it.
        s3AsyncClient.close();
      }
    });
    futureGet.join();
    long second = (System.currentTimeMillis() - start) / 1000;
    System.out.printf("Downloading time %s second %s MB/s throughput%n", second, 1.8 * 1024 / second);
    s3AsyncClient.close();
  }

  public static void getObjectWhole() throws Exception {
    String key = "alluxio-2.8.1-bin.tar.gz";
    String bucket = "lu-asf-demo";
    Region region = Region.US_EAST_1;
    S3Client s3 = S3Client.builder()
        .region(region)
        .credentialsProvider(DefaultCredentialsProvider.create())
        .build();

    GetObjectRequest objectRequest = GetObjectRequest
        .builder()
        .key(key)
        .bucket(bucket)
        .build();
    long start = System.currentTimeMillis();
    ResponseInputStream<GetObjectResponse> inputStream = s3.getObject(objectRequest);
    int batchSize = 4 * 1024 * 1024;
    byte[] buffer = new byte[batchSize];
    int read_len = 0;
    while ((read_len = inputStream.read(buffer)) > 0) {
      //
    }
    long end = System.currentTimeMillis();
    long second = (end - start) / 1000;
    System.out.printf("Downloading time %s second %s MB/s throughput%n", second, 1.8 * 1024 / second);
    // 92 MB/s
    // 92 MB/s
  }

  public static void readAsBytesWholeObject() {
    String key = "alluxio-2.8.1-bin.tar.gz";
    String bucket = "lu-asf-demo";
    Region region = Region.US_EAST_1;
    S3Client s3 = S3Client.builder()
        .region(region)
        .credentialsProvider(DefaultCredentialsProvider.create())
        .build();
    
    GetObjectRequest objectRequest = GetObjectRequest
        .builder()
        .key(key)
        .bucket(bucket)
        .build();
    long start = System.currentTimeMillis();
    ResponseBytes<GetObjectResponse> objectBytes = s3.getObjectAsBytes(objectRequest);
    byte[] data = objectBytes.asByteArray();
    long end = System.currentTimeMillis();
    long second = (end - start) / 1000;
    System.out.printf("Downloading time %s second %s MB/s throughput%n", second, 1.8 * 1024 / second);
    // 80 MB/s
  }

}
