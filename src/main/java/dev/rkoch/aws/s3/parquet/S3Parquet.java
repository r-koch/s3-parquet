package dev.rkoch.aws.s3.parquet;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.apache.parquet.schema.MessageType;
import blue.strategic.parquet.Dehydrator;
import blue.strategic.parquet.ParquetWriter;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3Parquet {

  private final S3Client s3Client;

  public S3Parquet() {
    this(Region.EU_WEST_1);
  }

  public S3Parquet(Region region) {
    this.s3Client = S3Client.builder().region(region).build();
  }

  public void write(final String bucket, final String key, final List<? extends ParquetRecord> records) throws Exception {
    File file = Files.createTempFile(null, null).toFile();
    ParquetRecord first = records.getFirst();
    /**
     * https://github.com/strategicblue/parquet-floor/blob/master/src/test/java/blue/strategic/parquet/ParquetReadWriteTest.java
     */
    MessageType schema = first.getSchema();
    Dehydrator<ParquetRecord> dehydrator = first.getDehydrator();
    try (ParquetWriter<ParquetRecord> writer = ParquetWriter.writeFile(schema, file, dehydrator)) {
      for (ParquetRecord record : records) {
        writer.write(record);
      }
    }
    s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromFile(file));
  }

}
