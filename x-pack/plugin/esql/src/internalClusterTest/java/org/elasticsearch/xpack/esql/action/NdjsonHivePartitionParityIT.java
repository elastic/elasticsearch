/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/** NDJSON leg of the hive-partition parity sweep. */
public class NdjsonHivePartitionParityIT extends AbstractHivePartitionParityIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    @Override
    protected String extension() {
        return "ndjson";
    }

    @Override
    protected void writeFlat(Path file, List<int[]> rows) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int[] r : rows) {
            sb.append("{\"id\":")
                .append(r[2])
                .append(",\"region\":\"")
                .append(REGION[r[0]])
                .append("\",\"tier\":\"")
                .append(TIER[r[1]])
                .append("\",\"w\":")
                .append(r[3])
                .append(",\"x\":")
                .append(r[4])
                .append(",\"y\":")
                .append(r[5])
                .append(",\"z\":")
                .append(r[6])
                .append("}\n");
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
    }

    @Override
    protected void writePartitioned(Path file, List<int[]> rows) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int[] r : rows) {
            sb.append("{\"id\":")
                .append(r[2])
                .append(",\"w\":")
                .append(r[3])
                .append(",\"x\":")
                .append(r[4])
                .append(",\"y\":")
                .append(r[5])
                .append(",\"z\":")
                .append(r[6])
                .append("}\n");
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
    }
}
