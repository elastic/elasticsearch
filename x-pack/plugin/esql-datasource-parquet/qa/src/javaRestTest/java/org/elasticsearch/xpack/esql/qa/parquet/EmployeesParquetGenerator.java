/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.parquet;

import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Builds an in-memory Parquet blob with the same three-row {@code employees} shape that the
 * CSV {@code FromDatasetSubquery*RestIT} variants use (Alice/Bob/Carol with emp_no 1..3), so
 * the parquet variants here can reuse the exact same query and assertion contract.
 *
 * <p>Uses {@link PlainParquetConfiguration} and a custom {@link OutputFile} backed by a
 * {@link ByteArrayOutputStream} so no Hadoop {@code FileSystem} / {@code Configuration}
 * classes are touched at runtime — matching the same Hadoop-free pattern used by
 * {@code ExternalSourceProfileIT} / {@code ExternalParquetTopNExtractionIT} in
 * {@code esql/internalClusterTest}.
 */
public final class EmployeesParquetGenerator {

    private EmployeesParquetGenerator() {}

    /**
     * Returns parquet bytes encoding three rows with schema
     * {@code emp_no:INT32, first_name:UTF8, last_name:UTF8, salary:INT32}, emp_no 1..3.
     * UTF8-annotated BINARY surfaces as ESQL {@code keyword}, INT32 as ESQL {@code integer}.
     */
    public static byte[] sampleEmployeesParquetBytes() throws IOException {
        return employeesParquetBytes(
            new EmployeeRow(1, "Alice", "Anderson", 50000),
            new EmployeeRow(2, "Bob", "Brown", 60000),
            new EmployeeRow(3, "Carol", "Cox", 55000)
        );
    }

    /**
     * Returns parquet bytes with the same {@code employees} schema as {@link #sampleEmployeesParquetBytes()}
     * but a disjoint emp_no range (101..103) so a multi-subquery test can prove which side a row came
     * from purely from the {@code emp_no} value.
     */
    public static byte[] alternateEmployeesParquetBytes() throws IOException {
        return employeesParquetBytes(
            new EmployeeRow(101, "Dave", "Davis", 70000),
            new EmployeeRow(102, "Eve", "Edwards", 65000),
            new EmployeeRow(103, "Frank", "Foster", 80000)
        );
    }

    /** Row holder used purely to keep {@link #employeesParquetBytes(EmployeeRow...)} call sites readable. */
    public record EmployeeRow(int empNo, String firstName, String lastName, int salary) {}

    /** A row for {@link #eventLogParquetBytes(EventRow...)}: an id, a keyword category, and an epoch-millis event time. */
    public record EventRow(int id, String category, long eventTimeMillis) {}

    /**
     * Returns parquet bytes with schema {@code id:INT32, category:UTF8, event_time:INT64 TIMESTAMP(MILLIS, UTC)}.
     * The timestamp-annotated INT64 is <em>inferred</em> as ES|QL {@code date} with no declared mapping — the column a
     * dashboard time picker filters on. Used by the request-filter parity test to prove an out-of-band {@code range} on
     * an inferred date selects the same rows on a dataset as on a mirror index.
     */
    public static byte[] eventLogParquetBytes(EventRow... rows) throws IOException {
        MessageType schema = new MessageType(
            "events",
            Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("id"),
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("category"),
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("event_time")
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = byteArrayOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (EventRow r : rows) {
                Group g = factory.newGroup();
                g.add("id", r.id);
                g.add("category", r.category);
                g.add("event_time", r.eventTimeMillis);
                writer.write(g);
            }
        }
        return baos.toByteArray();
    }

    /**
     * Writes the supplied rows under the canonical 4-column employees schema and returns the
     * resulting parquet bytes. Exposed so additional REST ITs can build their own fixtures without
     * duplicating the writer plumbing.
     */
    public static byte[] employeesParquetBytes(EmployeeRow... rows) throws IOException {
        MessageType schema = new MessageType(
            "employees",
            Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("emp_no"),
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("first_name"),
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("last_name"),
            Types.required(PrimitiveType.PrimitiveTypeName.INT32).named("salary")
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        OutputFile outputFile = byteArrayOutputFile(baos);
        SimpleGroupFactory factory = new SimpleGroupFactory(schema);

        try (
            ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputFile)
                .withConf(new PlainParquetConfiguration())
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
        ) {
            for (EmployeeRow r : rows) {
                Group g = factory.newGroup();
                g.add("emp_no", r.empNo);
                g.add("first_name", r.firstName);
                g.add("last_name", r.lastName);
                g.add("salary", r.salary);
                writer.write(g);
            }
        }
        return baos.toByteArray();
    }

    private static OutputFile byteArrayOutputFile(ByteArrayOutputStream baos) {
        return new OutputFile() {
            @Override
            public PositionOutputStream create(long blockSizeHint) {
                return positionOutputStream(baos);
            }

            @Override
            public PositionOutputStream createOrOverwrite(long blockSizeHint) {
                return create(blockSizeHint);
            }

            @Override
            public boolean supportsBlockSize() {
                return false;
            }

            @Override
            public long defaultBlockSize() {
                return 0;
            }
        };
    }

    private static PositionOutputStream positionOutputStream(ByteArrayOutputStream baos) {
        return new PositionOutputStream() {
            private long position = 0;

            @Override
            public long getPos() {
                return position;
            }

            @Override
            public void write(int b) throws IOException {
                baos.write(b);
                position++;
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                baos.write(b, off, len);
                position += len;
            }
        };
    }
}
