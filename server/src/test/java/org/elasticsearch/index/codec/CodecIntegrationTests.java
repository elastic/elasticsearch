/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.test.ESSingleNodeTestCase;

import static org.elasticsearch.index.codec.CodecTests.getLucene90StoredFieldsFormatMode;
import static org.hamcrest.Matchers.equalTo;

public class CodecIntegrationTests extends ESSingleNodeTestCase {

    public void testCanConfigureLegacySettings() {
        createIndex("index1", Settings.builder().put("index.codec", "legacy_default").build());
        var codec = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, "index1")
            .execute()
            .actionGet()
            .getSetting("index1", "index.codec");
        assertThat(codec, equalTo("legacy_default"));

        createIndex("index2", Settings.builder().put("index.codec", "legacy_best_compression").build());
        codec = client().admin()
            .indices()
            .prepareGetSettings(TEST_REQUEST_TIMEOUT, "index2")
            .execute()
            .actionGet()
            .getSetting("index2", "index.codec");
        assertThat(codec, equalTo("legacy_best_compression"));
    }

    public void testDefaultCodecLogsdb() {
        var indexService = createIndex("index1", Settings.builder().put("index.mode", "logsdb").build());
        var storedFieldsFormat = (Zstd814StoredFieldsFormat) indexService.getShard(0)
            .getEngineOrNull()
            .config()
            .getCodec()
            .storedFieldsFormat();
        assertThat(storedFieldsFormat.getMode(), equalTo(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION));
    }

    public void testDefaultCodec() throws Exception {
        var indexService = createIndex("index1");
        var storedFieldsFormat = (Lucene90StoredFieldsFormat) indexService.getShard(0)
            .getEngineOrNull()
            .config()
            .getCodec()
            .storedFieldsFormat();
        var mode = getLucene90StoredFieldsFormatMode(storedFieldsFormat);
        assertThat(mode, equalTo(Lucene90StoredFieldsFormat.Mode.BEST_SPEED));
    }
}
