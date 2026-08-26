/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;

import java.util.Collection;
import java.util.List;

/** CSV binding of {@link AbstractExternalReadConfigParityIT}. */
public class CsvExternalReadConfigParityIT extends AbstractExternalReadConfigParityIT {

    @Override
    protected String format() {
        return "csv";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected String fileExtension() {
        return ".csv";
    }

    @Override
    protected String buildContent(int rows) {
        StringBuilder sb = new StringBuilder("id:integer,age:keyword\n");
        for (int i = 0; i < rows; i++) {
            sb.append(i).append(',').append(20 + i % 50).append('\n');
        }
        return sb.toString();
    }
}
