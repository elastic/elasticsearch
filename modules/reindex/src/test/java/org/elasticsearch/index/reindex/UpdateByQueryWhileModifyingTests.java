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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;

/**
 * Mutates a document while update-by-query-ing it and asserts that the mutation
 * always sticks. Update-by-query should never revert documents.
 */
public class UpdateByQueryWhileModifyingTests extends ReindexTestCase {
    private static final int MAX_MUTATIONS = 50;
    private static final int MAX_ATTEMPTS = 50;

    public void testUpdateWhileReindexing() throws Exception {
        AtomicReference<String> value = new AtomicReference<>(randomSimpleString(random()));
        indexRandom(true, client().prepareIndex("test", "test", "test").setSource("test", value.get()));

        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicBoolean keepUpdating = new AtomicBoolean(true);
        Thread updater = new Thread(() -> {
            while (keepUpdating.get()) {
                try {
                    BulkByScrollResponse response = updateByQuery().source("test").refresh(true).abortOnVersionConflict(false).get();
                    assertThat(response, matcher().updated(either(equalTo(0L)).or(equalTo(1L)))
                            .versionConflicts(either(equalTo(0L)).or(equalTo(1L))));
                } catch (Exception e) {
                    failure.set(e);
                }
            }
        });
        updater.start();

        try {
            for (int i = 0; i < MAX_MUTATIONS; i++) {
                GetResponse get = client().prepareGet("test", "test", "test").get();
                assertEquals(value.get(), get.getSource().get("test"));
                value.set(randomSimpleString(random()));
                IndexRequestBuilder index = client().prepareIndex("test", "test", "test").setSource("test", value.get())
                        .setRefreshPolicy(IMMEDIATE);
                /*
                 * Update by query changes the document so concurrent
                 * indexes might get version conflict exceptions so we just
                 * blindly retry.
                 */
                int attempts = 0;
                while (true) {
                    attempts++;
                    try {
                        index.setIfSeqNo(get.getSeqNo()).setIfPrimaryTerm(get.getPrimaryTerm()).get();
                        break;
                    } catch (VersionConflictEngineException e) {
                        if (attempts >= MAX_ATTEMPTS) {
                            throw new RuntimeException(
                                    "Failed to index after [" + MAX_ATTEMPTS + "] attempts. Too many version conflicts!");
                        }
                        logger.info("Caught expected version conflict trying to perform mutation number [{}] with version [{}] "
                                + "on attempt [{}]. Retrying.", i, get.getVersion(), attempts);
                        get = client().prepareGet("test", "test", "test").get();
                    }
                }
            }
        } finally {
            keepUpdating.set(false);
            updater.join(TimeUnit.SECONDS.toMillis(10));
            if (failure.get() != null) {
                throw new RuntimeException(failure.get());
            }
        }
    }

}
