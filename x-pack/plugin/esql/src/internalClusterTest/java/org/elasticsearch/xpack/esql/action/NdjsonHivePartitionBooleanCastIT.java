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

/** NDJSON leg of the boolean hive-partition cast guard. */
public class NdjsonHivePartitionBooleanCastIT extends AbstractHivePartitionBooleanCastIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    @Override
    protected String extension() {
        return "ndjson";
    }

    @Override
    protected void writeIdRows(Path file, List<Integer> ids) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int id : ids) {
            sb.append("{\"id\":").append(id).append("}\n");
        }
        Files.writeString(file, sb.toString(), StandardCharsets.UTF_8);
    }
}
