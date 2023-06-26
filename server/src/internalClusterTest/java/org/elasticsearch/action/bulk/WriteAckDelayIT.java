/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.ArrayList;
import java.util.List;

public class WriteAckDelayIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(WriteAckDelay.WRITE_ACK_DELAY_INTERVAL.getKey(), TimeValue.timeValueMillis(50));
        settings.put(WriteAckDelay.WRITE_ACK_DELAY_RANDOMNESS_BOUND.getKey(), TimeValue.timeValueMillis(20));
        return settings.build();
    }

    public void testIndexWithWriteDelayEnabled() throws Exception {
        createIndex("test");
        int numOfDocs = randomIntBetween(100, 400);
        logger.info("indexing [{}] docs", numOfDocs);
        List<IndexRequestBuilder> builders = new ArrayList<>(numOfDocs);
        for (int j = 0; j < numOfDocs; j++) {
            builders.add(client().prepareIndex("test").setSource("field", "value_" + j));
        }
        indexRandom(true, builders);
        logger.info("verifying indexed content");
        int numOfChecks = randomIntBetween(8, 12);
        for (int j = 0; j < numOfChecks; j++) {
            try {
                logger.debug("running search");
                SearchResponse response = client().prepareSearch("test").get();
                if (response.getHits().getTotalHits().value != numOfDocs) {
                    final String message = "Count is "
                        + response.getHits().getTotalHits().value
                        + " but "
                        + numOfDocs
                        + " was expected. "
                        + ElasticsearchAssertions.formatShardStatus(response);
                    logger.error("{}. search response: \n{}", message, response);
                    fail(message);
                }
            } catch (Exception e) {
                logger.error("search failed", e);
                throw e;
            }
        }
    }
}
