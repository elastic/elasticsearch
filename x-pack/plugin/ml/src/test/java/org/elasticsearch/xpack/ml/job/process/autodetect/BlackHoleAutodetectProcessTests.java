/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.output.FlushAcknowledgement;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class BlackHoleAutodetectProcessTests extends ESTestCase {

    public void testFlushJob_writesAck() throws Exception {
        try (BlackHoleAutodetectProcess process = new BlackHoleAutodetectProcess("foo", failureReason -> {})) {
            String flushId = process.flushJob(FlushJobParams.builder().build());
            Iterator<AutodetectResult> iterator = process.readAutodetectResults();
            assertTrue(iterator.hasNext());
            AutodetectResult result = iterator.next();
            FlushAcknowledgement ack = result.getFlushAcknowledgement();
            assertEquals(flushId, ack.getId());
        }
    }

    public void testSimulatedFailure() throws Exception {
        AtomicReference<String> failureReason = new AtomicReference<>();
        try (BlackHoleAutodetectProcess process = new BlackHoleAutodetectProcess("foo", failureReason::set)) {
            Iterator<AutodetectResult> iterator = process.readAutodetectResults();
            process.writeRecord(new String[] { BlackHoleAutodetectProcess.MAGIC_FAILURE_VALUE});
            assertFalse(process.isProcessAlive());
            assertTrue(iterator.hasNext());
            AutodetectResult result = iterator.next();
            assertThat(result.getModelSizeStats(), nullValue());
            assertThat(result.getBucket(), nullValue());
            assertThat(result.getFlushAcknowledgement(), nullValue());
            assertThat(result.getCategoryDefinition(), nullValue());
            assertThat(result.getModelSnapshot(), nullValue());
            assertThat(result.getQuantiles(), nullValue());
            assertThat(result.getInfluencers(), nullValue());
            assertThat(result.getModelPlot(), nullValue());
            assertThat(result.getRecords(), nullValue());
            assertThat(result.getForecast(), nullValue());
            assertThat(result.getForecastRequestStats(), nullValue());
            assertFalse(iterator.hasNext());
        }
        assertThat(failureReason.get(), equalTo("simulated failure"));
    }
}
