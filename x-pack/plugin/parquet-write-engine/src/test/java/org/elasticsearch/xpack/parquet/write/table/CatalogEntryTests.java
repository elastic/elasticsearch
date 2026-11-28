/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.table;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class CatalogEntryTests extends ESTestCase {

    private static CatalogEntry entry(List<String> namespace, String previous) {
        return new CatalogEntry(
            namespace,
            "logs-parquet",
            "5b1c6a4e-0000-4000-8000-000000000001",
            "s3://bucket/base/logs-parquet/abc",
            "s3://bucket/base/logs-parquet/abc/metadata/v7.metadata.json",
            previous,
            7,
            1_763_200_000_000L,
            1_763_203_600_000L,
            "logs-parquet",
            "abc",
            "cluster-1"
        );
    }

    public void testAnEntryIsReadBackAsItWasWritten() throws Exception {
        final CatalogEntry entry = entry(List.of("default"), "s3://bucket/base/logs-parquet/abc/metadata/v6.metadata.json");
        assertThat(CatalogEntry.parse(entry.toBytes()), equalTo(entry));
    }

    public void testTheFirstEntryHasNoPreviousMetadataFile() throws Exception {
        final CatalogEntry entry = entry(List.of("default"), null);
        assertThat(entry.toBytes().utf8ToString(), not(containsString("previous_metadata_location")));
        assertThat(CatalogEntry.parse(entry.toBytes()).previousMetadataLocation(), nullValue());
    }

    public void testANestedNamespaceIsKept() throws Exception {
        final CatalogEntry entry = entry(List.of("prod", "logs"), null);
        assertThat(CatalogEntry.parse(entry.toBytes()).namespace(), equalTo(List.of("prod", "logs")));
    }

    public void testFieldsThatThisVersionDoesNotKnowAreIgnored() throws Exception {
        final String json = entry(List.of("default"), null).toBytes().utf8ToString().replaceFirst("\\{", "{\"added_later\": {\"a\": 1}, ");
        assertThat(CatalogEntry.parse(new BytesArray(json)), equalTo(entry(List.of("default"), null)));
    }

    public void testAnEntryOfANewerFormatVersionIsRefused() throws Exception {
        final String json = entry(List.of("default"), null).toBytes()
            .utf8ToString()
            .replace("\"format_version\" : 1", "\"format_version\" : 2");
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CatalogEntry.parse(new BytesArray(json)));
        assertThat(e.getMessage(), containsString("format version [2]"));
    }

    public void testAFileOfAnotherFormatIsRefused() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CatalogEntry.parse(new BytesArray("{\"format\": \"something-else\"}"))
        );
        assertThat(e.getMessage(), containsString("not a catalog entry"));
    }
}
