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

/** Shared writer for the delimited text legs (CSV, TSV) of the boolean hive-partition cast guard. */
public abstract class AbstractDelimitedHivePartitionBooleanCastIT extends AbstractHivePartitionBooleanCastIT {

    /** The field delimiter ({@code ','} for CSV, {@code '\t'} for TSV) separating the two data columns. */
    protected abstract char delimiter();

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected void writeIdRows(Path file, List<Integer> ids) throws IOException {
        // Two data columns (id, n) so the format's delimiter is actually exercised; the test reads only id and the
        // path-derived flag, so the extra column is harmless.
        char d = delimiter();
        StringBuilder sb = new StringBuilder("id").append(d).append("n").append('\n');
        for (int id : ids) {
            sb.append(id).append(d).append(id * 10).append('\n');
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
    }
}
