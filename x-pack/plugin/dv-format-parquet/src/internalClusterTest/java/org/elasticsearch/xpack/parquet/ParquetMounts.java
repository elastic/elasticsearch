/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.docvalues.ParquetTestFiles;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

/** Test helpers: a read-only {@code fs} repository that holds a Parquet file, and the settings of an index that mounts it. */
final class ParquetMounts {

    static final String REPOSITORY = "lake";
    static final String PATH = "events/part-0001.parquet";
    static final int ROWS = 3_500;
    static final int ROWS_PER_ROW_GROUP = 1_000;

    /** The mapping of the fields that the mount serves. */
    static final String MAPPING = "{\"properties\":{\"status\":{\"type\":\"long\",\"index\":false,\"doc_values\":true},"
        + "\"ts\":{\"type\":\"long\",\"index\":false,\"doc_values\":true}}}";

    private ParquetMounts() {}

    /** Registers the repository and writes a flat file with {@link #ROWS} rows into it, and returns the location of the repository. */
    static Path writeFileAndRegisterRepository(ESIntegTestCase test) throws IOException {
        Path location = ESIntegTestCase.randomRepoPath();
        Path file = location.resolve(PATH);
        Files.createDirectories(file.getParent());
        Files.write(file, ParquetTestFiles.writeFlat(ROWS, ROWS_PER_ROW_GROUP));
        assertAcked(
            ESIntegTestCase.client()
                .admin()
                .cluster()
                .preparePutRepository(ESIntegTestCase.TEST_REQUEST_TIMEOUT, ESIntegTestCase.TEST_REQUEST_TIMEOUT, REPOSITORY)
                .setType("fs")
                .setSettings(Settings.builder().put("location", location).put("readonly", true))
        );
        return location;
    }

    /** The settings that mount the file and serve its {@code id} column as the field {@code status} and {@code ts} as {@code ts}. */
    static Settings.Builder mountSettings() {
        return mountSettings("status:id,ts:ts");
    }

    /** The settings that mount the file and serve the {@code field:column} pairs. */
    static Settings.Builder mountSettings(String fields) {
        return Settings.builder()
            .put(ParquetBackedSettings.REPOSITORY.getKey(), REPOSITORY)
            .putList(ParquetBackedSettings.PIECES.getKey(), PATH + "#0+" + (ROWS / ROWS_PER_ROW_GROUP + 1))
            .put(ParquetBackedSettings.FIELDS.getKey(), fields);
    }

    /**
     * The fields of the columnar mount: numbers, one of them optional, and strings, one of them optional. A string is a {@code keyword}
     * of a columnar index, which keeps its single value as the raw bytes in binary doc values, and does not allow several values.
     */
    static final String COLUMNAR_FIELDS = "status:id,ts:ts,maybe:maybe,name:name,kind:kind,note:note";

    static final String COLUMNAR_MAPPING = "{\"properties\":{"
        + "\"status\":{\"type\":\"long\",\"index\":false},"
        + "\"ts\":{\"type\":\"long\",\"index\":false},"
        + "\"maybe\":{\"type\":\"long\",\"index\":false},"
        + "\"name\":{\"type\":\"keyword\",\"index\":false,\"multi_value\":false},"
        + "\"kind\":{\"type\":\"keyword\",\"index\":false,\"multi_value\":false},"
        + "\"note\":{\"type\":\"keyword\",\"index\":false,\"multi_value\":false}}}";
}
