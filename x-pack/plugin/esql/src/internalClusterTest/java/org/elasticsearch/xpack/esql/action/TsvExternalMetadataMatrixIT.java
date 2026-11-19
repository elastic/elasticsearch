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

/**
 * TSV binding of the standard-metadata matrix. Shares {@link CsvDataSourcePlugin} with the CSV
 * binding — the same {@code CsvFormatReader} backs both formats; only the default delimiter and
 * the file extension differ. Pinning a named {@code tsv} cell ensures the matrix's "works on
 * every supported format" claim is verifiable from the test list without tracing the dispatch.
 */
public class TsvExternalMetadataMatrixIT extends AbstractExternalMetadataMatrixIT {

    @Override
    protected String format() {
        return "tsv";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    @Override
    protected String writeFixture(Path dir) throws Exception {
        Path file = dir.resolve("employees.tsv");
        Files.writeString(
            file,
            String.join(
                "\n",
                "emp_no:integer\tfirst_name:keyword\thost_ip:keyword",
                "1\tAlice\t10.0.0.1",
                "2\tBob\t10.0.0.2",
                "3\tCarol\t10.0.0.3"
            ) + "\n"
        );
        return StoragePath.fileUri(file);
    }
}
