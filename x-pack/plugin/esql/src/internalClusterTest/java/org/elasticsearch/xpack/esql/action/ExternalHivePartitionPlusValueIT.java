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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end guard that a Hive partition value holding a literal {@code +} is surfaced unchanged and remains
 * queryable. Hive partition folders are {@code %XX}-escaped only and write {@code +} literally, so a folder
 * {@code tag=a+b/} must surface {@code tag = "a+b"}; decoding the value the {@code application/x-www-form-urlencoded}
 * way turns the {@code +} into a space, so {@code WHERE tag == "a+b"} matches nothing and silently drops the rows.
 */
public class ExternalHivePartitionPlusValueIT extends AbstractExternalDataSourceIT {

    @Override
    protected Collection<Class<? extends Plugin>> formatPlugins() {
        return List.of(CsvDataSourcePlugin.class);
    }

    public void testLiteralPlusPartitionValueIsQueryable() throws IOException {
        Path root = createTempDir().resolve("hive_plus");
        writeTagRow(root, "a+b", 1);
        writeTagRow(root, "c", 2);

        @SuppressWarnings("checkstyle:EmptyJavadoc") // the glob's '/**/' is misread as Javadoc
        String glob = StoragePath.fileUri(root) + "/**/*.csv";
        String dataset = registerDataset("hive_plus", glob, Map.of("hive_partitioning", true));

        var request = syncEsqlQueryRequest("FROM " + dataset + " | WHERE tag == \"a+b\" | KEEP id | SORT id ASC");
        try (var response = run(request)) {
            List<Long> ids = getValuesList(response).stream().map(row -> ((Number) row.get(0)).longValue()).toList();
            assertThat("WHERE tag == \"a+b\" must return the tag=a+b folder's row, not drop it", ids, equalTo(List.of(1L)));
        }
    }

    /** Writes {@code root/tag=<tag>/f.csv} carrying one row with a single {@code id} column. */
    private static void writeTagRow(Path root, String tag, int id) throws IOException {
        Path dir = root.resolve("tag=" + tag);
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("f.csv"), "id\n" + id + "\n", StandardCharsets.UTF_8);
    }
}
