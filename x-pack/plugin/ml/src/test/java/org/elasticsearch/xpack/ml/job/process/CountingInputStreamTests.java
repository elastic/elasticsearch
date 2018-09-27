/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.mock.orig.Mockito.when;
import static org.mockito.Mockito.mock;

public class CountingInputStreamTests extends ESTestCase {

    private ClusterService clusterService;

    @Before
    public void setUpMocks() {
        Settings settings = Settings.builder().put(DataCountsReporter.MAX_ACCEPTABLE_PERCENT_OF_DATE_PARSE_ERRORS_SETTING.getKey(), 10)
            .put(DataCountsReporter.MAX_ACCEPTABLE_PERCENT_OF_OUT_OF_ORDER_ERRORS_SETTING.getKey(), 10)
            .build();
        Set<Setting<?>> setOfSettings = new HashSet<>();
        setOfSettings.add(DataCountsReporter.MAX_ACCEPTABLE_PERCENT_OF_DATE_PARSE_ERRORS_SETTING);
        setOfSettings.add(DataCountsReporter.MAX_ACCEPTABLE_PERCENT_OF_OUT_OF_ORDER_ERRORS_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(settings, setOfSettings);

        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
    }

    public void testRead_OneByteAtATime() throws IOException {

        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter(clusterService);

        final String TEXT = "123";
        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            while (counting.read() >= 0) {}
            assertEquals(TEXT.length(), dataCountsReporter.incrementalStats().getInputBytes());
        }
    }

    public void testRead_WithBuffer() throws IOException {
        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";

        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter(clusterService);

        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            byte buf[] = new byte[256];
            while (counting.read(buf) >= 0) {}
            assertEquals(TEXT.length(), dataCountsReporter.incrementalStats().getInputBytes());
        }
    }

    public void testRead_WithTinyBuffer() throws IOException {
        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";

        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter(clusterService);

        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            byte buf[] = new byte[8];
            while (counting.read(buf, 0, 8) >= 0) {}
            assertEquals(TEXT.length(), dataCountsReporter.incrementalStats().getInputBytes());
        }
    }

    public void testRead_WithResets() throws IOException {

        DummyDataCountsReporter dataCountsReporter = new DummyDataCountsReporter(clusterService);

        final String TEXT = "To the man who only has a hammer, everything he encounters begins to look like a nail.";
        InputStream source = new ByteArrayInputStream(TEXT.getBytes(StandardCharsets.UTF_8));

        try (CountingInputStream counting = new CountingInputStream(source, dataCountsReporter)) {
            while (counting.read() >= 0) {
                if (randomInt(10) > 5) {
                    counting.mark(-1);
                }
                if (randomInt(10) > 7) {
                    counting.reset();
                }
            }
            assertEquals(TEXT.length(), dataCountsReporter.incrementalStats().getInputBytes());
        }
    }

}
