package alluxio;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.File;
import java.io.FileOutputStream;

public final class Main {

  public static void main(String[] args) throws Exception {
    String key = "alluxio-2.8.1-bin.tar.gz";
    String bucket = "lu-asf-demo";
    System.out.format("Downloading %s from S3 bucket %s...\n", key, bucket);
    final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
    long start = System.currentTimeMillis();
    S3Object object = s3.getObject(bucket, key);
    S3ObjectInputStream s3is = object.getObjectContent();
    FileOutputStream fos = new FileOutputStream(new File(key));
    byte[] read_buf = new byte[128 * 1024]; // 128KB each read and write
    int read_len = 0;
    while ((read_len = s3is.read(read_buf)) > 0) {
      fos.write(read_buf, 0, read_len);
    }
    s3is.close();
    fos.close();
    long end = System.currentTimeMillis();
    long second = (end - start) / 1000; 
    System.out.printf("Downloading time %s second %s MB/s throughput%n", second, 1.8 * 1024 / second);
    System.exit(0);
  }
}
