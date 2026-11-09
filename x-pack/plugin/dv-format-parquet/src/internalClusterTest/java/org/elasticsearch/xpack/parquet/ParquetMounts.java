/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.parquet.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.mount.ParquetMountRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/** Test helpers: a read-only {@code fs} repository that holds a Parquet file, the mount of it, and a search that reads its columns. */
final class ParquetMounts {

    static final String REPOSITORY = "lake";
    static final String PATH = "events/part-0001.parquet";
    static final String INDEX = "events";
    static final int ROWS = 3_500;
    static final int ROWS_PER_ROW_GROUP = 1_000;

    private ParquetMounts() {}

    /** Writes a flat file with {@link #ROWS} rows into a new read-only {@code fs} repository and registers it. */
    static void writeFileAndRegisterRepository() throws IOException {
        Path location = ESIntegTestCase.randomRepoPath();
        Path file = location.resolve(PATH);
        Files.createDirectories(file.getParent());
        Files.write(file, ParquetSampleFiles.writeFlat(ROWS, ROWS_PER_ROW_GROUP));
        assertAcked(
            ESIntegTestCase.client()
                .admin()
                .cluster()
                .preparePutRepository(ESIntegTestCase.TEST_REQUEST_TIMEOUT, ESIntegTestCase.TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", location).put("readonly", true))
        );
    }

    /** Mounts the file as the index {@link #INDEX}, with no replica (the mount decides the number of shards). */
    static void mount(ESIntegTestCase test) {
        Map<String, Object> body = Map.of(
            "repository",
            REPOSITORY,
            "paths",
            List.of(PATH),
            "index",
            INDEX,
            "index_settings",
            Map.of("index.number_of_replicas", 0)
        );
        ESIntegTestCase.client().execute(MountParquetAction.INSTANCE, new ParquetMountRequest(body)).actionGet();
        test.ensureGreen(INDEX);
    }

    /** A search that reads the numeric columns of the mount and checks what the file holds. */
    static void assertServesTheFile() {
        assertResponse(
            ESIntegTestCase.prepareSearch(INDEX)
                .setSize(0)
                .setTrackTotalHits(true)
                .addAggregation(AggregationBuilders.max("max_id").field("id"))
                .addAggregation(AggregationBuilders.sum("sum_ts").field("ts")),
            response -> {
                org.junit.Assert.assertEquals(ROWS, response.getHits().getTotalHits().value());
                org.junit.Assert.assertEquals(ROWS - 1, ((Max) response.getAggregations().get("max_id")).value(), 0.0);
                org.junit.Assert.assertEquals(
                    1000.0 * ROWS * (ROWS - 1) / 2,
                    ((Sum) response.getAggregations().get("sum_ts")).value(),
                    0.0
                );
            }
        );
    }
}
