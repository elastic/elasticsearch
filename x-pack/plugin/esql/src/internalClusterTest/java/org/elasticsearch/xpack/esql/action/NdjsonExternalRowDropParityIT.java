/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;

import java.util.Collection;
import java.util.List;

/** NDJSON binding of {@link AbstractExternalRowDropParityIT}. */
public class NdjsonExternalRowDropParityIT extends AbstractExternalRowDropParityIT {

    @Override
    protected String format() {
        return "ndjson";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    @Override
    protected String fileExtension() {
        return ".ndjson";
    }

    @Override
    protected String buildContent(int rows, boolean malformed) {
        int badRow = rows / 2;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows; i++) {
            if (malformed && i == badRow) {
                sb.append("not-a-json-object\n"); // unparseable line -> dropped
            } else {
                sb.append("{\"id\":").append(i).append(",\"name\":\"row_").append(i).append("\",\"value\":").append(i + 0.5).append("}\n");
            }
        }
        return sb.toString();
    }
}
