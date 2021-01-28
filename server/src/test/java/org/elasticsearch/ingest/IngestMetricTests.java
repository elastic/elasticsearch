/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
