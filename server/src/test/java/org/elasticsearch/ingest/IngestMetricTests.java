/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class IngestMetricTests extends ESTestCase {

    public void testIngestCurrent() {
        IngestMetric metric = new IngestMetric();
        metric.preIngest();
        assertThat(1L, equalTo(metric.createStats().getIngestCurrent()));
        metric.postIngest(0);
        assertThat(0L, equalTo(metric.createStats().getIngestCurrent()));
    }

    public void testIngestTimeInNanos() {
        IngestMetric metric = new IngestMetric();
        metric.preIngest();
        metric.postIngest(500000L);
        metric.preIngest();
        metric.postIngest(500000L);
        assertThat(1L, equalTo(metric.createStats().getIngestTimeInMillis()));
    }

}
