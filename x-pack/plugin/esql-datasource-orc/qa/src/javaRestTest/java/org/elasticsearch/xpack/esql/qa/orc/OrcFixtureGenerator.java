/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.orc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.SuppressForbidden;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Generates the employees.orc fixture file used by ORC integration tests.
 * Reads the canonical employees.csv from the CSV datasource module to ensure
 * the ORC fixture data matches exactly.
 */
public final class OrcFixtureGenerator {

    private static final String CSV_RESOURCE = "/iceberg-fixtures/standalone/employees.csv";

    private OrcFixtureGenerator() {}

    @SuppressForbidden(reason = "main method for Gradle JavaExec task needs System.out and Path.of")
    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.err.println("Usage: OrcFixtureGenerator <output-path>");
            System.exit(1);
        }
        java.nio.file.Path outputPath = java.nio.file.Path.of(args[0]);
        java.nio.file.Files.createDirectories(outputPath.getParent());
        java.nio.file.Files.deleteIfExists(outputPath);
        generate(outputPath);
        System.out.println("Generated ORC fixture: " + outputPath);
    }

    /**
     * Generates an ORC file at the given path with employee test data read from the canonical CSV.
     */
    public static void generate(java.nio.file.Path outputPath) throws IOException {
        // Schema matches the CSV: emp_no, first_name, last_name, birth_date, gender, hire_date,
        // languages, languages.long, height, height.float, height.scaled_float, height.half_float,
        // salary, still_hired, avg_worked_seconds
        TypeDescription schema = TypeDescription.createStruct()
            .addField("emp_no", TypeDescription.createInt())
            .addField("first_name", TypeDescription.createString())
            .addField("last_name", TypeDescription.createString())
            .addField("birth_date", TypeDescription.createTimestampInstant())
            .addField("gender", TypeDescription.createString())
            .addField("hire_date", TypeDescription.createTimestampInstant())
            .addField("languages", TypeDescription.createInt())
            .addField("languages.long", TypeDescription.createLong())
            .addField("height", TypeDescription.createDouble())
            .addField("height.float", TypeDescription.createDouble())
            .addField("height.scaled_float", TypeDescription.createDouble())
            .addField("height.half_float", TypeDescription.createDouble())
            .addField("salary", TypeDescription.createInt())
            .addField("still_hired", TypeDescription.createBoolean())
            .addField("avg_worked_seconds", TypeDescription.createLong());

        Configuration conf = new Configuration(false);
        NoChmodFileSystem localFs = new NoChmodFileSystem();
        localFs.setConf(conf);

        Path orcPath = new Path(outputPath.toUri());
        OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(conf)
            .setSchema(schema)
            .fileSystem(localFs)
            .compress(CompressionKind.NONE);

        try (Writer writer = OrcFile.createWriter(orcPath, writerOptions)) {
            VectorizedRowBatch batch = schema.createRowBatch(100);
            LongColumnVector empNo = (LongColumnVector) batch.cols[0];
            BytesColumnVector firstName = (BytesColumnVector) batch.cols[1];
            BytesColumnVector lastName = (BytesColumnVector) batch.cols[2];
            TimestampColumnVector birthDate = (TimestampColumnVector) batch.cols[3];
            BytesColumnVector gender = (BytesColumnVector) batch.cols[4];
            TimestampColumnVector hireDate = (TimestampColumnVector) batch.cols[5];
            LongColumnVector languages = (LongColumnVector) batch.cols[6];
            LongColumnVector languagesLong = (LongColumnVector) batch.cols[7];
            DoubleColumnVector height = (DoubleColumnVector) batch.cols[8];
            DoubleColumnVector heightFloat = (DoubleColumnVector) batch.cols[9];
            DoubleColumnVector heightScaledFloat = (DoubleColumnVector) batch.cols[10];
            DoubleColumnVector heightHalfFloat = (DoubleColumnVector) batch.cols[11];
            LongColumnVector salary = (LongColumnVector) batch.cols[12];
            LongColumnVector stillHired = (LongColumnVector) batch.cols[13];
            LongColumnVector avgWorkedSeconds = (LongColumnVector) batch.cols[14];

            try (InputStream is = OrcFixtureGenerator.class.getResourceAsStream(CSV_RESOURCE)) {
                if (is == null) {
                    throw new IOException("CSV resource not found: " + CSV_RESOURCE);
                }
                BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                reader.readLine(); // skip header

                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.isBlank()) {
                        continue;
                    }
                    String[] fields = line.split(",", -1);
                    int r = batch.size++;

                    empNo.vector[r] = Integer.parseInt(fields[0]);
                    setString(firstName, r, fields[1]);
                    setString(lastName, r, fields[2]);
                    setTimestamp(birthDate, r, fields[3]);
                    setString(gender, r, fields[4]);
                    setTimestamp(hireDate, r, fields[5]);
                    setNullableInt(languages, r, fields[6]);
                    setNullableLong(languagesLong, r, fields[7]);
                    height.vector[r] = Double.parseDouble(fields[8]);
                    heightFloat.vector[r] = Double.parseDouble(fields[9]);
                    heightScaledFloat.vector[r] = Double.parseDouble(fields[10]);
                    heightHalfFloat.vector[r] = Double.parseDouble(fields[11]);
                    salary.vector[r] = Integer.parseInt(fields[12]);
                    stillHired.vector[r] = Booleans.parseBoolean(fields[13]) ? 1 : 0;
                    avgWorkedSeconds.vector[r] = Long.parseLong(fields[14]);
                }
            }

            writer.addRowBatch(batch);
        }
    }

    private static void setString(BytesColumnVector col, int row, String value) {
        if (value.isEmpty()) {
            col.noNulls = false;
            col.isNull[row] = true;
        } else {
            col.setVal(row, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void setTimestamp(TimestampColumnVector col, int row, String value) {
        if (value.isEmpty()) {
            col.noNulls = false;
            col.isNull[row] = true;
        } else {
            long millis = Instant.parse(value).toEpochMilli();
            col.time[row] = millis;
            col.nanos[row] = 0;
        }
    }

    private static void setNullableInt(LongColumnVector col, int row, String value) {
        if (value.isEmpty()) {
            col.noNulls = false;
            col.isNull[row] = true;
        } else {
            col.vector[row] = Integer.parseInt(value);
        }
    }

    private static void setNullableLong(LongColumnVector col, int row, String value) {
        if (value.isEmpty()) {
            col.noNulls = false;
            col.isNull[row] = true;
        } else {
            col.vector[row] = Long.parseLong(value);
        }
    }

    /**
     * Minimal FileSystem that skips chmod calls blocked by Elasticsearch's entitlement system.
     */
    private static class NoChmodFileSystem extends RawLocalFileSystem {
        @Override
        public void setPermission(Path p, FsPermission permission) {
            // no-op
        }
    }
}
