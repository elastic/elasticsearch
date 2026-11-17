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
                // An extra column, not a non-numeric id: a wrong-width row is a STRUCTURAL drop, decided while
                // tokenising and so independent of what the query projects. A coercion drop would be
                // projection-dependent, and those suppress the stats publish entirely (see CsvFormatReader's
                // projectionDependentDrop), which is the behaviour this suite is not about.
                sb.append(i).append(",row_").append(i).append(',').append(i + 0.5).append(",extra").append('\n');
            } else {
                sb.append(i).append(",row_").append(i).append(',').append(i + 0.5).append('\n');
            }
        }
        return sb.toString();
    }
}
