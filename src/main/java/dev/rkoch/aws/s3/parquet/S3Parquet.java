package dev.rkoch.aws.s3.parquet;

import java.io.File;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.parquet.schema.MessageType;
import blue.strategic.parquet.Dehydrator;
import blue.strategic.parquet.Hydrator;
import blue.strategic.parquet.HydratorSupplier;
import blue.strategic.parquet.ParquetReader;
import blue.strategic.parquet.ParquetWriter;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3Parquet {

  private final S3Client s3Client;

  public S3Parquet() {
    this(Region.EU_WEST_1);
  }

  public S3Parquet(Region region) {
    this.s3Client = S3Client.builder().region(region).build();
  }

  public <T extends ParquetRecord> List<T> read(final String bucket, final String key, final Hydrator<T, T> hydrator) throws Exception {
    Path path = Files.createTempFile(null, null);
    try (ResponseInputStream<GetObjectResponse> inputStream = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
        OutputStream outputStream = Files.newOutputStream(path)) {
      inputStream.transferTo(outputStream);
    }
    return ParquetReader.streamContent(path.toFile(), HydratorSupplier.constantly(hydrator)).toList();
  }

  public <T extends ParquetRecord> void write(final String bucket, final String key, final List<T> records, final Dehydrator<T> dehydrator) throws Exception {
    File file = Files.createTempFile(null, null).toFile();
    T first = records.getFirst();
    /**
     * https://github.com/strategicblue/parquet-floor/blob/master/src/test/java/blue/strategic/parquet/ParquetReadWriteTest.java
     */
    MessageType schema = first.getSchema();
    try (ParquetWriter<T> writer = ParquetWriter.writeFile(schema, file, dehydrator)) {
      for (T record : records) {
        writer.write(record);
      }
    }
    s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), RequestBody.fromFile(file));
  }

}
