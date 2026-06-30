/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/** CSV binding of {@link AbstractExternalAggregatePushdownMatrixIT}. */
public class CsvExternalAggregatePushdownMatrixIT extends AbstractExternalAggregatePushdownMatrixIT {

    @Override
    protected String format() {
        return "csv";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected String writeFixture(Path dir, int rows) throws Exception {
        StringBuilder sb = new StringBuilder("emp_no:long,label:keyword,val:double\n");
        for (int i = 0; i < rows; i++) {
            sb.append(i).append(',').append(label(i)).append(',').append(i + 0.5).append('\n');
        }
        Path file = dir.resolve("employees.csv");
        Files.writeString(file, sb.toString());
        return StoragePath.fileUri(file);
    }
}
