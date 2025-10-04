package dev.rkoch.aws.s3.parquet;

import org.apache.parquet.schema.MessageType;
import blue.strategic.parquet.Dehydrator;
import blue.strategic.parquet.Hydrator;

public interface ParquetRecord {

  Dehydrator<? extends ParquetRecord> getDehydrator();

  Hydrator<? extends ParquetRecord, ? extends ParquetRecord> getHydrator();

  MessageType getSchema();

}
