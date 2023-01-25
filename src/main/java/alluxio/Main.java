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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class Main {

  public static void main(String[] args) throws Exception {
    transferManager();
    getObjectWhole();
    readAsBytesWholeObject();
    asyncClientWholeObject();
    System.exit(0);
  }

  public static void transferManager() throws Exception {
    String key = "alluxio-2.8.1-bin.tar.gz";
    String bucket = "lu-asf-demo";
    Region region = Region.US_EAST_1;
    S3AsyncClient s3AsyncClient =
        S3AsyncClient.crtBuilder()
            .credentialsProvider(DefaultCredentialsProvider.create())
            .region(Region.US_EAST_1)
            .targetThroughputInGbps(20.0)
            .minimumPartSizeInBytes(8 * 1024L * 1024)
            .build();

    ExecutorService service = Executors.newFixedThreadPool(20);
    S3TransferManager transferManager =
        S3TransferManager.builder()
            .s3Client(s3AsyncClient)
            .executor(service)
            .build();

    DownloadFileRequest downloadFileRequest =
        DownloadFileRequest.builder()
            .getObjectRequest(b -> b.bucket(bucket).key(key))
            .destination(new File(key))
            .build();
    long start = System.currentTimeMillis();
    FileDownload downloadFile = transferManager.downloadFile(downloadFileRequest);
    CompletedFileDownload downloadResult = downloadFile.completionFuture().join();
    long end = System.currentTimeMillis();
    long second = (end - start) / 1000;
    System.out.printf("Downloading time %s second %s MB/s throughput%n", second, 1.8 * 1024 / second);
    // 83MB/s default one without executor
    s3AsyncClient.close();
    service.shutdown();
    transferManager.close();
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

  public static void asyncClientWholeObject() {
    String key = "alluxio-2.8.1-bin.tar.gz";
    String bucket = "lu-asf-demo";
    ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.create();
    Region region = Region.US_EAST_1;
    S3AsyncClient s3AsyncClient = S3AsyncClient.builder()
        .region(region)
        .credentialsProvider(credentialsProvider)
        .build();
    GetObjectRequest objectRequest = GetObjectRequest
        .builder()
        .key(key)
        .bucket(bucket)
        .build();
    long start = System.currentTimeMillis();
    CompletableFuture<GetObjectResponse> futureGet = s3AsyncClient.getObject(objectRequest,
        AsyncResponseTransformer.toFile(new File(key)));
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
    long end = System.currentTimeMillis();
    long second = (end - start) / 1000;
    System.out.printf("Downloading time %s second %s MB/s throughput%n", second, 1.8 * 1024 / second);
  }
}
