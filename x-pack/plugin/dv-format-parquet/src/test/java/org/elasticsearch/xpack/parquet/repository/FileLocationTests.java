/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.repository;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class FileLocationTests extends ESTestCase {

    public void testALocationOfARepositoryReadsBack() {
        FileLocation location = new RepositoryLocation("lake", "events/a.parquet");
        assertEquals("repository://lake/events/a.parquet", location.toString());
        assertEquals(location, FileLocation.parse(location.toString()));
        assertEquals("lake", location.sourceName());
    }

    public void testALocationOfADataSourceReadsBack() {
        DataSourceLocation location = new DataSourceLocation("clickbench", "s3://bucket/dir/a.parquet");
        assertEquals("datasource://clickbench/s3://bucket/dir/a.parquet", location.toString());
        FileLocation parsed = FileLocation.parse(location.toString());
        assertThat(parsed, instanceOf(DataSourceLocation.class));
        assertEquals(location, parsed);
        assertEquals("clickbench", parsed.sourceName());
        assertEquals("s3", location.storagePath().scheme());
    }

    public void testLocationsThatAreNotValidAreRejected() {
        expectThrows(IllegalArgumentException.class, () -> FileLocation.parse("s3://bucket/a.parquet"));
        expectThrows(IllegalArgumentException.class, () -> FileLocation.parse("datasource://clickbench"));
        expectThrows(IllegalArgumentException.class, () -> FileLocation.parse("datasource:///s3://bucket/a.parquet"));
        expectThrows(IllegalArgumentException.class, () -> new DataSourceLocation("", "s3://bucket/a.parquet"));
        expectThrows(IllegalArgumentException.class, () -> new DataSourceLocation("a/b", "s3://bucket/a.parquet"));
        expectThrows(IllegalArgumentException.class, () -> new DataSourceLocation("clickbench", "bucket/a.parquet"));
        expectThrows(IllegalArgumentException.class, () -> new DataSourceLocation("clickbench", "s3://bucket/../a.parquet"));
        expectThrows(IllegalArgumentException.class, () -> new DataSourceLocation("clickbench", "s3://bucket/a.parquet#0"));
    }

    public void testTheSettingsNameARepositoryOrADataSourceButNotBoth() {
        Settings repository = Settings.builder().put(ParquetBackedSettings.REPOSITORY.getKey(), "lake").build();
        assertEquals(new RepositoryLocation("lake", "a.parquet"), ParquetBackedSettings.location(repository, "a.parquet"));
        assertTrue(ParquetBackedSettings.isParquetBacked(repository));

        Settings dataSource = Settings.builder().put(ParquetBackedSettings.DATA_SOURCE.getKey(), "clickbench").build();
        assertEquals(
            new DataSourceLocation("clickbench", "s3://bucket/a.parquet"),
            ParquetBackedSettings.location(dataSource, "s3://bucket/a.parquet")
        );
        assertTrue(ParquetBackedSettings.isParquetBacked(dataSource));

        Settings both = Settings.builder().put(repository).put(dataSource).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> ParquetBackedSettings.location(both, "a.parquet"));
        assertThat(e.getMessage(), containsString("exactly one of"));
        expectThrows(IllegalArgumentException.class, () -> ParquetBackedSettings.location(Settings.EMPTY, "a.parquet"));
        assertFalse(ParquetBackedSettings.isParquetBacked(Settings.EMPTY));
    }
}
