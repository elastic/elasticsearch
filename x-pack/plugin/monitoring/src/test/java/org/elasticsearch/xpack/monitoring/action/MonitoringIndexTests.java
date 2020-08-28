/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.monitoring.action.MonitoringIndex;

import java.io.IOException;

/**
 * Tests {@link MonitoringIndex}
 */
public class MonitoringIndexTests extends ESTestCase {

    public void testDataMatchesIndexName() {
        assertFalse(MonitoringIndex.IGNORED_DATA.matchesIndexName("_data"));
        assertFalse(MonitoringIndex.IGNORED_DATA.matchesIndexName("_DATA"));
        assertFalse(MonitoringIndex.IGNORED_DATA.matchesIndexName("_dAtA"));
        assertFalse(MonitoringIndex.IGNORED_DATA.matchesIndexName("_data "));
        assertFalse(MonitoringIndex.IGNORED_DATA.matchesIndexName(" _data "));
        assertFalse(MonitoringIndex.IGNORED_DATA.matchesIndexName(""));
        assertFalse(MonitoringIndex.IGNORED_DATA.matchesIndexName(null));
    }

    public void testTimestampMatchesIndexName() {
        assertTrue(MonitoringIndex.TIMESTAMPED.matchesIndexName(""));
        assertTrue(MonitoringIndex.TIMESTAMPED.matchesIndexName(null));
        assertFalse(MonitoringIndex.TIMESTAMPED.matchesIndexName(" "));
        assertFalse(MonitoringIndex.TIMESTAMPED.matchesIndexName("_data"));
    }

    public void testFrom() {
        assertSame(MonitoringIndex.IGNORED_DATA, MonitoringIndex.from("_data"));
        assertSame(MonitoringIndex.TIMESTAMPED, MonitoringIndex.from(""));
        assertSame(MonitoringIndex.TIMESTAMPED, MonitoringIndex.from(null));
    }

    public void testFromFails() {
        String[] invalidNames = { "_DATA", "other", "    " };

        for (String name : invalidNames) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> MonitoringIndex.from(name));

            assertEquals("unrecognized index name [" + name + "]", e.getMessage());
        }
    }

    public void testStreaming() throws IOException {
        MonitoringIndex index = randomFrom(MonitoringIndex.values());

        final BytesStreamOutput out = new BytesStreamOutput();

        index.writeTo(out);

        final StreamInput in = out.bytes().streamInput();

        assertSame(index, MonitoringIndex.readFrom(in));

        assertEquals(0, in.available());

        in.close();
        out.close();
    }

}
