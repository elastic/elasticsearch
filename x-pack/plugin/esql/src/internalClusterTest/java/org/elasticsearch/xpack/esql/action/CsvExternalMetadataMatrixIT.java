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

/** CSV binding of the standard-metadata matrix. The CSV reader is not {@code ColumnExtractorAware}. */
public class CsvExternalMetadataMatrixIT extends AbstractExternalMetadataMatrixIT {

    @Override
    protected String format() {
        return "csv";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected String writeFixture(Path dir) throws Exception {
        Path file = dir.resolve("employees.csv");
        Files.writeString(
            file,
            String.join("\n", "emp_no:integer,first_name:keyword,host_ip:keyword", "1,Alice,10.0.0.1", "2,Bob,10.0.0.2", "3,Carol,10.0.0.3")
                + "\n"
        );
        return StoragePath.fileUri(file);
    }
}
