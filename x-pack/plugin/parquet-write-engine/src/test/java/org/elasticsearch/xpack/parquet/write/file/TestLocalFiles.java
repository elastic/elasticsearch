/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.file;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;

import java.io.IOException;
import java.util.List;

/** Writes local Parquet files the way the engine does, for the tests of the layers above it. */
public final class TestLocalFiles {

    private TestLocalFiles() {}

    public static final long BASE_TIMESTAMP = 1_700_000_000_000L;

    /** The schema of a bulk: the row number, a timestamp and a keyword, and optionally an extra long column added by a mapping update. */
    public static MessageType schema(boolean withExtraColumn) {
        final Types.MessageTypeBuilder builder = Types.buildMessage();
        builder.addField(Types.optional(PrimitiveTypeName.INT64).named("count"));
        builder.addField(
            Types.optional(PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("@timestamp")
        );
        builder.addField(Types.optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("host.name"));
        if (withExtraColumn) {
            builder.addField(Types.optional(PrimitiveTypeName.INT64).named("extra"));
        }
        return builder.named("doc");
    }

    /** Rows {@code from} to {@code to} inclusive, named by the sequence numbers like a local file of the engine. */
    public static LocalParquetFiles.LocalFile write(Directory directory, long from, long to, boolean withExtraColumn) throws IOException {
        final MessageType schema = schema(withExtraColumn);
        final SimpleGroupFactory factory = new SimpleGroupFactory(schema);
        final String name = LocalParquetFiles.name(from, to);
        try (IndexOutput out = directory.createOutput(name, IOContext.DEFAULT)) {
            try (
                ParquetWriter<Group> writer = ExampleParquetWriter.builder(new BatchParquetWriter.IndexOutputFile(out))
                    .withConf(new PlainParquetConfiguration())
                    .withCodecFactory(new ParquetCodecs())
                    .withType(schema)
                    .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                    .build()
            ) {
                for (long row = from; row <= to; row++) {
                    final Group group = factory.newGroup();
                    group.add("count", row);
                    group.add("@timestamp", BASE_TIMESTAMP + row * 1000);
                    // every seventh row has no keyword: a null
                    if (row % 7 != 0) {
                        group.add("host.name", Binary.fromString("host-" + (row % 5)));
                    }
                    if (withExtraColumn) {
                        group.add("extra", row * 2);
                    }
                    writer.write(group);
                }
            }
        }
        return new LocalParquetFiles.LocalFile(name, from, to, directory.fileLength(name));
    }

    public static List<LocalParquetFiles.LocalFile> list(Directory directory) throws IOException {
        return LocalParquetFiles.list(directory);
    }
}
