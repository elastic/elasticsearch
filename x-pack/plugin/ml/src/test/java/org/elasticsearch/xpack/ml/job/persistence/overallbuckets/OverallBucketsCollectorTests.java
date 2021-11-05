/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence.overallbuckets;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.results.OverallBucket;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class OverallBucketsCollectorTests extends ESTestCase {

    public void testProcess_GivenEmpty() {
        OverallBucketsCollector collector = new OverallBucketsCollector();
        collector.process(Collections.emptyList());
        assertThat(collector.finish().isEmpty(), is(true));
    }

    public void testProcess_GivenTwoBatches() {
        OverallBucket b1 = mock(OverallBucket.class);
        OverallBucket b2 = mock(OverallBucket.class);
        OverallBucket b3 = mock(OverallBucket.class);
        OverallBucket b4 = mock(OverallBucket.class);
        OverallBucket b5 = mock(OverallBucket.class);

        OverallBucketsCollector collector = new OverallBucketsCollector();
        collector.process(Arrays.asList(b1, b2, b3));
        collector.process(Arrays.asList(b4, b5));

        assertThat(collector.finish(), equalTo(Arrays.asList(b1, b2, b3, b4, b5)));
    }
}
