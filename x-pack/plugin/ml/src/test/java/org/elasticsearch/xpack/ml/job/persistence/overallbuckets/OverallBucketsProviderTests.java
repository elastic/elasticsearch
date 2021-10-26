/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.persistence.overallbuckets;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.persistence.overallbuckets.OverallBucketsProvider.TopNScores;

import static org.hamcrest.Matchers.equalTo;

public class OverallBucketsProviderTests extends ESTestCase {

    public void testTopNScores() {
        TopNScores topNScores = new TopNScores(3);
        assertThat(topNScores.overallScore(), equalTo(0.0));
        topNScores.insertWithOverflow(5.0);
        assertThat(topNScores.overallScore(), equalTo(5.0));
        topNScores.insertWithOverflow(4.0);
        assertThat(topNScores.overallScore(), equalTo(4.5));
        topNScores.insertWithOverflow(3.0);
        assertThat(topNScores.overallScore(), equalTo(4.0));
        topNScores.insertWithOverflow(6.0);
        assertThat(topNScores.overallScore(), equalTo(5.0));
        topNScores.insertWithOverflow(10.0);
        assertThat(topNScores.overallScore(), equalTo(7.0));
    }
}
