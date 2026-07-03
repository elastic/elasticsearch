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
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.util.Random;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class ColumnarRuntimeFieldsValidationIT extends ESIntegTestCase {

    private static final String MAPPING_WITH_RUNTIME_FIELD = """
        {
          "properties": {
            "kwd": { "type": "keyword" }
          },
          "runtime": {
            "runtime_kwd": { "type": "keyword" }
          }
        }
        """;

    private static final String MAPPING_WITHOUT_RUNTIME_FIELD = """
        {
          "properties": {
            "kwd": { "type": "keyword" }
          }
        }
        """;

    private static final String RUNTIME_FIELD_PUT_MAPPING_BODY = """
        {
          "runtime": {
            "runtime_kwd": { "type": "keyword" }
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

    public void testColumnarCreateIndexRejectsRuntimeField() {
        assertCreateIndexRejected(IndexMode.COLUMNAR, "test_columnar_create_runtime");
    }

    public void testColumnarPutMappingRejectsRuntimeField() {
        assertPutMappingRejected(IndexMode.COLUMNAR, "test_columnar_put_runtime");
    }

    public void testLogsdbColumnarCreateIndexRejectsRuntimeField() {
        assertCreateIndexRejected(IndexMode.LOGSDB_COLUMNAR, "test_logsdb_columnar_create_runtime");
    }

    public void testLogsdbColumnarPutMappingRejectsRuntimeField() {
        assertPutMappingRejected(IndexMode.LOGSDB_COLUMNAR, "test_logsdb_columnar_put_runtime");
    }

    private void assertCreateIndexRejected(IndexMode mode, String index) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(index).setSettings(modeSettings(mode)).setMapping(MAPPING_WITH_RUNTIME_FIELD).get()
        );
        assertThat(e.getMessage(), containsString("mapping-level runtime fields are not allowed in index using [" + mode + "] index mode"));
    }

    private void assertPutMappingRejected(IndexMode mode, String index) {
        assertAcked(prepareCreate(index).setSettings(modeSettings(mode)).setMapping(MAPPING_WITHOUT_RUNTIME_FIELD));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> indicesAdmin().preparePutMapping(index).setSource(RUNTIME_FIELD_PUT_MAPPING_BODY, XContentType.JSON).get()
        );
        assertThat(e.getMessage(), containsString("mapping-level runtime fields are not allowed in index using [" + mode + "] index mode"));
    }

    private static Settings modeSettings(IndexMode mode) {
        return Settings.builder().put(IndexSettings.MODE.getKey(), mode.getName()).build();
    }
}
