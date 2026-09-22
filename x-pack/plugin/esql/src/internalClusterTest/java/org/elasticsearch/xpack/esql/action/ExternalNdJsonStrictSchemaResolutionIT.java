/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.datasource.ndjson.NdJsonDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.contains;

/**
 * End-to-end coverage for strict schema resolution when sparse NDJSON records make identical
 * logical schemas appear in different inferred column orders across files.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExternalNdJsonStrictSchemaResolutionIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(NdJsonDataSourcePlugin.class);
    }

    public void testStrictMapsSparseFilesByColumnName() throws Exception {
        Path dir = createTempDir().resolve("strict_ndjson_sparse");
        Files.createDirectories(dir);
        Files.writeString(
            dir.resolve("a.ndjson"),
            "{\"id\":1,\"error_code\":10,\"level\":\"INFO\"}\n" + "{\"id\":2,\"error_code\":20,\"level\":\"DEBUG\"}\n",
            StandardCharsets.UTF_8
        );
        Files.writeString(
            dir.resolve("b.ndjson"),
            "{\"id\":3,\"level\":\"WARN\"}\n" + "{\"id\":4,\"error_code\":40,\"level\":\"ERROR\"}\n",
            StandardCharsets.UTF_8
        );

        String glob = StoragePath.fileUri(dir) + "/*.ndjson";
        String dataset = registerDataset("strict_ndjson_sparse", glob, Map.of("schema_resolution", "strict"));

        try (var response = run(syncEsqlQueryRequest("FROM " + dataset + " | SORT id | KEEP error_code, level"))) {
            assertThat(
                getValuesList(response),
                contains(List.of(10, "INFO"), List.of(20, "DEBUG"), Arrays.asList(null, "WARN"), List.of(40, "ERROR"))
            );
        }
    }
}
