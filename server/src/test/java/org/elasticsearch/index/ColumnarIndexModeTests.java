/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ColumnarIndexModeTests extends ESTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("columnar index mode requires snapshot build", IndexMode.COLUMNAR_FEATURE_FLAG.isEnabled());
    }

    public void testColumnarFromString() {
        assertThat(IndexMode.fromString("columnar"), equalTo(IndexMode.COLUMNAR));
        assertThat(IndexMode.fromString("COLUMNAR"), equalTo(IndexMode.COLUMNAR));
    }

    public void testColumnarSerialization() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexMode.writeTo(IndexMode.COLUMNAR, out);
            try (var in = out.bytes().streamInput()) {
                assertThat(IndexMode.readFrom(in), equalTo(IndexMode.COLUMNAR));
            }
        }
    }

    public void testColumnarIndexModeSetting() {
        Settings settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build();
        assertThat(IndexSettings.MODE.get(settings), equalTo(IndexMode.COLUMNAR));
    }

    public void testColumnarDefaultSourceMode() {
        assertThat(IndexMode.COLUMNAR.defaultSourceMode(), equalTo(SourceFieldMapper.Mode.SYNTHETIC));
    }

    public void testColumnarGetName() {
        assertThat(IndexMode.COLUMNAR.getName(), equalTo("columnar"));
        assertThat(IndexMode.COLUMNAR.toString(), equalTo("columnar"));
    }

    public void testColumnarShouldValidateTimestamp() {
        assertThat(IndexMode.COLUMNAR.shouldValidateTimestamp(), equalTo(false));
    }

    public void testColumnarTimestampBound() {
        final IndexMetadata metadata = IndexSettingsTests.newIndexMeta(
            "test",
            Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName()).build()
        );
        assertThat(IndexMode.COLUMNAR.getTimestampBound(metadata), equalTo(null));
    }

    public void testIsColumnar() {
        assertThat(IndexMode.STANDARD.isColumnar(), equalTo(false));
        assertThat(IndexMode.TIME_SERIES.isColumnar(), equalTo(true));
        assertThat(IndexMode.LOGSDB.isColumnar(), equalTo(true));
        assertThat(IndexMode.COLUMNAR.isColumnar(), equalTo(true));
        assertThat(IndexMode.COLUMNAR_LOGSDB.isColumnar(), equalTo(true));
        assertThat(IndexMode.LOOKUP.isColumnar(), equalTo(false));
    }
}
