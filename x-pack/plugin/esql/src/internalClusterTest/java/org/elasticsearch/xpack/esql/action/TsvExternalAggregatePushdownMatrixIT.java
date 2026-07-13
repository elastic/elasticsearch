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

/** TSV binding of {@link AbstractExternalAggregatePushdownMatrixIT} (TSV shares the CSV reader plugin). */
public class TsvExternalAggregatePushdownMatrixIT extends AbstractExternalAggregatePushdownMatrixIT {

    @Override
    protected String format() {
        return "tsv";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected String writeFixture(Path dir, int rows) throws Exception {
        StringBuilder sb = new StringBuilder("emp_no:long\tlabel:keyword\tval:double\n");
        for (int i = 0; i < rows; i++) {
            sb.append(i).append('\t').append(label(i)).append('\t').append(i + 0.5).append('\n');
        }
        Path file = dir.resolve("employees.tsv");
        Files.writeString(file, sb.toString());
        return StoragePath.fileUri(file);
    }
}
