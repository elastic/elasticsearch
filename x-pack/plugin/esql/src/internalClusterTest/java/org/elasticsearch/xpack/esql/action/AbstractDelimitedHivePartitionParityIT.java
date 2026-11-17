/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/** Shared writer for the delimited text legs (CSV, TSV) of the hive-partition parity sweep. */
public abstract class AbstractDelimitedHivePartitionParityIT extends AbstractHivePartitionParityIT {

    /** The field delimiter ({@code ','} for CSV, {@code '\t'} for TSV). */
    protected abstract char delimiter();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected void writeFlat(Path file, List<int[]> rows) throws IOException {
        char d = delimiter();
        StringBuilder sb = new StringBuilder("id").append(d)
            .append("region")
            .append(d)
            .append("tier")
            .append(d)
            .append("w")
            .append(d)
            .append("x")
            .append(d)
            .append("y")
            .append(d)
            .append("z")
            .append('\n');
        for (int[] r : rows) {
            sb.append(r[2])
                .append(d)
                .append(REGION[r[0]])
                .append(d)
                .append(TIER[r[1]])
                .append(d)
                .append(r[3])
                .append(d)
                .append(r[4])
                .append(d)
                .append(r[5])
                .append(d)
                .append(r[6])
                .append('\n');
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
    }

    @Override
    protected void writePartitioned(Path file, List<int[]> rows) throws IOException {
        char d = delimiter();
        StringBuilder sb = new StringBuilder("id").append(d)
            .append("w")
            .append(d)
            .append("x")
            .append(d)
            .append("y")
            .append(d)
            .append("z")
            .append('\n');
        for (int[] r : rows) {
            sb.append(r[2]).append(d).append(r[3]).append(d).append(r[4]).append(d).append(r[5]).append(d).append(r[6]).append('\n');
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
    }
}
