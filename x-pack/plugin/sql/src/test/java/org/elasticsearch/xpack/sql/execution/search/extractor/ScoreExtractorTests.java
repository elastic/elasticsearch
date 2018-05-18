/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search.extractor;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;

public class ScoreExtractorTests extends ESTestCase {
    public void testGet() {
        int times = between(1, 1000);
        for (int i = 0; i < times; i++) {
            float score = randomFloat();
            SearchHit hit = new SearchHit(1);
            hit.score(score);
            assertEquals(score, ScoreExtractor.INSTANCE.extract(hit));
        }
    }

    public void testToString() {
        assertEquals("SCORE", ScoreExtractor.INSTANCE.toString());
    }
}
