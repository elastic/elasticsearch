/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests of {@link AbstractColumnarMapperCompatibilityTestCase} itself: the contract the per-mapper parity tests rely on. A concrete
 * subclass is needed to run them; this one uses the simplest mappers available so that it says nothing about any one mapper.
 */
public class ColumnarCompatibilityHarnessTests extends AbstractColumnarMapperCompatibilityTestCase {

    private static final String FIELD = "f";

    private static Settings columnarSettings() {
        return Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
    }

    /** A batch the batch path accepts is compared with the row path and passes, so the refusals below are not the harness's default. */
    public void testAcceptedBatchPasses() throws IOException {
        assertColumnarMatchesXContent(
            mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
            columnarSettings(),
            batch("accepted", 1L, doc("d1", 1L, "{\"f\":\"hello\"}"), doc("d2", 2L, "{}"))
        );
    }

    /**
     * The harness resolves leaves with {@code ShardBatchMapper.resolveMappers}, so a batch production would send to the row path
     * fails loudly instead of being compared on whatever subset of leaves happened to be mappable. Here {@code extra} is unmapped
     * under the default {@code dynamic: true}, which the batch path cannot create a field for.
     */
    public void testRefusalForUnmappedLeafIsReported() throws IOException {
        final AssertionError e = expectThrows(
            AssertionError.class,
            () -> assertColumnarMatchesXContent(
                mapping(b -> b.startObject(FIELD).field("type", "keyword").endObject()),
                columnarSettings(),
                batch("unmapped dynamic leaf", 1L, doc("d1", 1L, "{\"f\":\"hello\",\"extra\":\"x\"}"))
            )
        );
        assertThat(e.getMessage(), containsString("production would fall back"));
        assertThat(e.getMessage(), containsString("unmapped dynamic leaf"));
        assertThat(e.getMessage(), containsString("leaf [extra] has no mapper"));
    }

    /** A leaf whose mapper does not support columnar parsing is named in the refusal, so the author knows what to change. */
    public void testRefusalForUnsupportedMapperNamesTheField() throws IOException {
        final AssertionError e = expectThrows(
            AssertionError.class,
            () -> assertColumnarMatchesXContent(
                mapping(b -> b.startObject(FIELD).field("type", "binary").endObject()),
                columnarSettings(),
                batch("unsupported mapper", 1L, doc("d1", 1L, "{\"f\":\"aGVsbG8=\"}"))
            )
        );
        assertThat(e.getMessage(), containsString("production would fall back"));
        assertThat(e.getMessage(), containsString("field [f]"));
        assertThat(e.getMessage(), containsString("does not support columnar parse"));
    }
}
