/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.cluster.metadata.DatasetFieldMapping;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasource.csv.CsvDataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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

    /**
     * A partition column that SHADOWS a real column in the files. This is the case that decides which schema the
     * serve gate may hash. The reader parses the file's own {@code region} column, so the harvest's fingerprint
     * includes it; the coordinator's unified schema prunes it in favour of the path-derived value. Hashing either
     * the partition-enriched schema or a naively data-only one gets this wrong in opposite directions -- only the
     * per-file overlaid physical schema, in the file's own column order, matches what the harvest hashed.
     * <p>
     * The shadowed column leads the header on purpose: the fingerprint hashes column order, so a leading shadow
     * cannot accidentally agree with a schema that dropped it.
     */
    public void testHiveShadowedColumnKeepsPerColumnWarmth() throws Exception {
        Path root = createTempDir();
        for (String region : new String[] { "east", "west" }) {
            Path dir = root.resolve("region=" + region);
            Files.createDirectories(dir);
            StringBuilder sb = new StringBuilder("region:keyword,id:integer,age:keyword\n");
            for (int i = 0; i < ROWS / 2; i++) {
                int id = region.equals("east") ? i : ROWS / 2 + i;
                // Payload values deliberately unlike the path values, so a read that took the path value is visible.
                sb.append("payload_").append(region).append(',').append(id).append(',').append(20 + id % 50).append('\n');
            }
            Files.writeString(dir.resolve("data.csv"), sb.toString());
        }
        String glob = StoragePath.fileUri(root) + "/*" + "*/*.csv";
        Map<String, Object> hive = Map.of("hive_partitioning", true);
        String inferred = register("shadow_inferred", glob, null, hive);
        String declared = register("shadow_declared", glob, mappingOf("age", new DatasetFieldMapping("keyword", null)), hive);

        assertMinScan(inferred, (long) ROWS);
        assertMinScan(inferred, 0L);
        assertMinScan(declared, 0L);
    }
}
