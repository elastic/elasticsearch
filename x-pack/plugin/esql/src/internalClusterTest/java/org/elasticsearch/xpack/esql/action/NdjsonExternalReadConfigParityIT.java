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

/** NDJSON binding of {@link AbstractExternalReadConfigParityIT}. */
public class NdjsonExternalReadConfigParityIT extends AbstractExternalReadConfigParityIT {

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
    protected String buildContent(int rows) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < rows; i++) {
            sb.append("{\"id\":").append(i).append(",\"age\":\"").append(20 + i % 50).append("\"}\n");
        }
        return sb.toString();
    }
}
