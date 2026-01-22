/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.versioning;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ConcurrentDocumentOperationIT extends ESIntegTestCase {
    public void testConcurrentOperationOnSameDoc() throws Exception {
        logger.info("--> create an index with 1 shard and max replicas based on nodes");
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", 1)));

        logger.info("execute concurrent updates on the same doc");
        int numberOfUpdates = 100;
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(numberOfUpdates);
        for (int i = 0; i < numberOfUpdates; i++) {
            prepareIndex("test").setId("1").setSource("field1", i).execute(new ActionListener<>() {
                @Override
                public void onResponse(DocWriteResponse response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("Unexpected exception while indexing", e);
                    failure.set(e);
                    latch.countDown();
                }
            });
        }

        latch.await();

        assertThat(failure.get(), nullValue());

        indicesAdmin().prepareRefresh().get();

        logger.info("done indexing, check all have the same field value");
        Map<String, Object> masterSource = client().prepareGet("test", "1").get().getSourceAsMap();
        for (int i = 0; i < (cluster().size() * 5); i++) {
            assertThat(client().prepareGet("test", "1").get().getSourceAsMap(), equalTo(masterSource));
        }
    }
}
