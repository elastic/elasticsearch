/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Map;
import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that {@code doc_values:false} is silently ignored for fields in strict columnar index modes
 * ({@code columnar} and {@code logsdb_columnar}). Fields must be indexed with doc values enabled so that
 * documents remain retrievable via synthetic source and are not silently dropped to {@code _ignored_source}.
 */
public class ColumnarDocValuesFlipIT extends ESIntegTestCase {

    private static final String MAPPING_DOC_VALUES_FALSE = """
        {
          "properties": {
            "kwd":  { "type": "keyword", "doc_values": false },
            "num":  { "type": "long",    "doc_values": false },
            "addr": { "type": "ip",      "doc_values": false }
          }
        }
        """;

    @Before
    public void checkFeatureFlag() {
        assumeTrue("columnar index modes require snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        // Columnar mode requires DOC_VALUES_ONLY for seq_no; remove the randomly-chosen value so
        // it doesn't conflict with the index-mode default (disable_sequence_numbers=true).
        return super.setRandomIndexSettings(random, builder).remove(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey());
    }

    public void testColumnarDocValuesFalseIsIgnored() {
        assertDocValuesFalseIgnored(IndexMode.COLUMNAR, "test_columnar_dv_flip");
    }

    public void testLogsdbColumnarDocValuesFalseIsIgnored() {
        assertDocValuesFalseIgnored(IndexMode.LOGSDB_COLUMNAR, "test_logsdb_columnar_dv_flip");
    }

    private void assertDocValuesFalseIgnored(IndexMode mode, String indexName) {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build();
        assertAcked(prepareCreate(indexName).setSettings(settings).setMapping(MAPPING_DOC_VALUES_FALSE));

        // Index a document with values for all three doc_values:false fields.
        // @timestamp is required for logsdb_columnar mode (inherits logsdb's timestamp requirement).
        prepareIndex(indexName).setId("1")
            .setSource(Map.of("kwd", "hello", "num", 42, "addr", "::1", "@timestamp", "2023-01-01T00:00:00.000Z"))
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();

        // Retrieve via _get — if doc values were disabled, synthetic source reconstruction would fail
        // or the field values would be missing.
        var source = client().prepareGet(indexName, "1").get().getSourceAsMap();
        assertThat("kwd field must be returned", source.get("kwd"), notNullValue());
        assertThat("num field must be returned", source.get("num"), notNullValue());
        assertThat("addr field must be returned", source.get("addr"), notNullValue());
        assertThat(source.get("kwd"), equalTo("hello"));
        assertThat(((Number) source.get("num")).longValue(), equalTo(42L));
        assertThat(source.get("addr"), equalTo("::1"));
    }
}
