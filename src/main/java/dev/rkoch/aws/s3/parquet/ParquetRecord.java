package dev.rkoch.aws.s3.parquet;

import org.apache.parquet.schema.MessageType;
import blue.strategic.parquet.Dehydrator;

public interface ParquetRecord {

  MessageType getSchema();

  Dehydrator<ParquetRecord> getDehydrator();

}
