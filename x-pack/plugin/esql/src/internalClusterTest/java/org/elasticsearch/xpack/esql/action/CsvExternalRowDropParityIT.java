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

/** CSV binding of {@link AbstractExternalRowDropParityIT}. */
public class CsvExternalRowDropParityIT extends AbstractExternalRowDropParityIT {

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
    protected String buildContent(int rows, boolean malformed) {
        int badRow = rows / 2;
        StringBuilder sb = new StringBuilder("id:integer,name:keyword,value:double\n");
        for (int i = 0; i < rows; i++) {
            if (malformed && i == badRow) {
                sb.append("notanint,row_").append(i).append(',').append(i + 0.5).append('\n'); // non-numeric id -> dropped
            } else {
                sb.append(i).append(",row_").append(i).append(',').append(i + 0.5).append('\n');
            }
        }
        return sb.toString();
    }
}
