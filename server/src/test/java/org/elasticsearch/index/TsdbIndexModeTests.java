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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@link IndexMode#TSDB} is a preferred alias for {@link IndexMode#TIME_SERIES}: both parse from
 * (and behave identically for) the same {@code index.mode} settings, but {@code time_series}
 * remains the canonical emitted/persisted string so existing indices are unaffected.
 */
public class TsdbIndexModeTests extends ESTestCase {

    public void testFromStringAcceptsBothSpellings() {
        assertThat(IndexMode.fromString("time_series"), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexMode.fromString("TIME_SERIES"), equalTo(IndexMode.TIME_SERIES));
        assertThat(IndexMode.fromString("tsdb"), equalTo(IndexMode.TSDB));
        assertThat(IndexMode.fromString("TSDB"), equalTo(IndexMode.TSDB));
        assertThat(IndexMode.fromString("Tsdb"), equalTo(IndexMode.TSDB));
    }

    public void testGetName() {
        assertThat(IndexMode.TIME_SERIES.getName(), equalTo("time_series"));
        assertThat(IndexMode.TIME_SERIES.toString(), equalTo("time_series"));
        assertThat(IndexMode.TSDB.getName(), equalTo("tsdb"));
        assertThat(IndexMode.TSDB.toString(), equalTo("tsdb"));
    }

    public void testIsTsdb() {
        assertTrue(IndexMode.TIME_SERIES.isTsdb());
        assertTrue(IndexMode.TSDB.isTsdb());
        for (IndexMode mode : IndexMode.values()) {
            if (mode != IndexMode.TIME_SERIES && mode != IndexMode.TSDB) {
                assertFalse(mode + " must not be tsdb", mode.isTsdb());
            }
        }
    }

    public void testIsTsdbNullSafeStatic() {
        assertTrue(IndexMode.isTsdb(IndexMode.TIME_SERIES));
        assertTrue(IndexMode.isTsdb(IndexMode.TSDB));
        assertFalse(IndexMode.isTsdb(IndexMode.STANDARD));
        assertFalse(IndexMode.isTsdb(null));
    }

    public void testIsTsdbName() {
        assertTrue(IndexMode.isTsdbName("time_series"));
        assertTrue(IndexMode.isTsdbName("TIME_SERIES"));
        assertTrue(IndexMode.isTsdbName("tsdb"));
        assertTrue(IndexMode.isTsdbName("TSDB"));
        assertFalse(IndexMode.isTsdbName("standard"));
        assertFalse(IndexMode.isTsdbName(null));
    }

    public void testIndexModeSettingAcceptsBothSpellings() {
        Settings timeSeriesSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "time_series")
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("uid"))
            .build();
        assertThat(IndexSettings.MODE.get(timeSeriesSettings), equalTo(IndexMode.TIME_SERIES));

        Settings tsdbSettings = Settings.builder()
            .put(IndexSettings.MODE.getKey(), "tsdb")
            .putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), List.of("uid"))
            .build();
        assertThat(IndexSettings.MODE.get(tsdbSettings), equalTo(IndexMode.TSDB));
    }

    public void testSerialization() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexMode.writeTo(IndexMode.TSDB, out);
            try (var in = out.bytes().streamInput()) {
                assertThat(IndexMode.readFrom(in), equalTo(IndexMode.TSDB));
            }
        }
        // TIME_SERIES's own wire representation is unaffected by adding TSDB.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            IndexMode.writeTo(IndexMode.TIME_SERIES, out);
            try (var in = out.bytes().streamInput()) {
                assertThat(IndexMode.readFrom(in), equalTo(IndexMode.TIME_SERIES));
            }
        }
    }

    public void testSerializationFailsOnOlderTransportVersion() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(IndexMode.INDEX_MODE_TSDB_ADDED));
            IllegalStateException e = expectThrows(IllegalStateException.class, () -> IndexMode.writeTo(IndexMode.TSDB, out));
            assertThat(e.getMessage(), containsString("[tsdb] doesn't support serialization with transport version"));
        }
    }

    /**
     * TSDB delegates every behavior to TIME_SERIES, so the two must remain indistinguishable
     * other than their name/identity.
     */
    public void testBehaviorParityWithTimeSeries() {
        assertThat(IndexMode.TSDB.defaultSourceMode(), equalTo(IndexMode.TIME_SERIES.defaultSourceMode()));
        assertThat(IndexMode.TSDB.isColumnar(), equalTo(IndexMode.TIME_SERIES.isColumnar()));
        assertThat(IndexMode.TSDB.isStrictColumnar(), equalTo(IndexMode.TIME_SERIES.isStrictColumnar()));
        assertThat(IndexMode.TSDB.shouldValidateTimestamp(), equalTo(IndexMode.TIME_SERIES.shouldValidateTimestamp()));
        assertThat(IndexMode.TSDB.supportedSourceModes(), equalTo(IndexMode.TIME_SERIES.supportedSourceModes()));
        assertThat(IndexMode.TSDB.getDefaultCodec(), equalTo(IndexMode.TIME_SERIES.getDefaultCodec()));
    }
}
