package alluxio;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class Main {

  public static void main(String[] args) throws Exception {
    transferManager();
    System.exit(0);
  }

  public static void transferManager() throws Exception {
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    ExecutorService service = Executors.newFixedThreadPool(20);
    TransferManager transferManager = TransferManagerBuilder.standard()
        .withS3Client(s3)
        .withExecutorFactory(() -> service).build();
    String key = "alluxio-2.8.1-bin.tar.gz";
    String bucket = "lu-asf-demo";
    long start = System.currentTimeMillis();
    Download download = transferManager.download(bucket, key, new File(key));
    download.waitForCompletion();
    long end = System.currentTimeMillis();
    long second = (end - start) / 1000;
    System.out.printf("Downloading time %s second %s MB/s throughput%n", second, 1.8 * 1024 / second);
    transferManager.shutdownNow();
  }
  
  public static void readSeparate() throws Exception {
    String key = "alluxio-2.8.1-bin.tar.gz";
    String bucket = "lu-asf-demo";
    System.out.format("Downloading %s from S3 bucket %s...\n", key, bucket);
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    long start = System.currentTimeMillis();
    GetObjectRequest request = new GetObjectRequest(bucket, key);

    int pos = 0;
    int batchSize = 4 * 1024 * 1024;
    S3Object object;
    byte[] buffer = new byte[batchSize];
    while (true) {
      try {
        request.setRange(pos, pos + batchSize - 1);
        object = s3.getObject(request);
      } catch (AmazonS3Exception e) {
        if (e.getStatusCode() == 416) {
          // InvalidRange exception when mPos >= file length
          break;
        }
        throw e;
      }
      try (S3ObjectInputStream in = object.getObjectContent()) {
        int currentRead = 0;
        int totalRead = 0;
        while (true) {
          currentRead = in.read(buffer, totalRead, batchSize - totalRead);
          if (currentRead <= 0) {
            break;
          }
          totalRead += currentRead;
        }
        pos += totalRead;
      }
    }
    long end = System.currentTimeMillis();
    long second = (end - start) / 1000;
    System.out.printf("Downloading time %s second %s MB/s throughput%n", second, 1.8 * 1024 / second);
  }

  public static void readWholeObject() throws Exception {
    String key = "alluxio-2.8.1-bin.tar.gz";
    String bucket = "lu-asf-demo";
    System.out.format("Downloading %s from S3 bucket %s...\n", key, bucket);
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    long start = System.currentTimeMillis();
    S3Object object = s3.getObject(bucket, key);
    S3ObjectInputStream s3is = object.getObjectContent();
    byte[] read_buf = new byte[4 * 1024 * 1024]; // 128KB each read and write
    int read_len = 0;
    while ((read_len = s3is.read(read_buf)) > 0) {
      //
    }
    s3is.close();
    long end = System.currentTimeMillis();
    long second = (end - start) / 1000;
    System.out.printf("Downloading time %s second %s MB/s throughput%n", second, 1.8 * 1024 / second);
  }
}
