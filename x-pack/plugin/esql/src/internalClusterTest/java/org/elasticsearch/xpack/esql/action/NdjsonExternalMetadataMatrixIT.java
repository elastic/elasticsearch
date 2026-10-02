/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

/**
 * NDJSON binding of the standard-metadata matrix. The NDJSON reader is not
 * {@code ColumnExtractorAware}; schema is inferred from the rows (small ints to {@code emp_no}:int,
 * quoted strings to keyword).
 */
public class NdjsonExternalMetadataMatrixIT extends AbstractExternalMetadataMatrixIT {

    @Override
    protected String format() {
        return "ndjson";
    }

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    @Override
    protected String writeFixture(Path dir) throws Exception {
        Path file = dir.resolve("employees.ndjson");
        Files.writeString(
            file,
            String.join(
                "\n",
                "{\"emp_no\":1,\"first_name\":\"Alice\",\"host_ip\":\"10.0.0.1\"}",
                "{\"emp_no\":2,\"first_name\":\"Bob\",\"host_ip\":\"10.0.0.2\"}",
                "{\"emp_no\":3,\"first_name\":\"Carol\",\"host_ip\":\"10.0.0.3\"}"
            ) + "\n"
        );
        return StoragePath.fileUri(file);
    }
}
