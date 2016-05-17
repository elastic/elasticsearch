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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.index.IndexRequestBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.elasticsearch.action.index.IndexRequest.OpType.CREATE;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests failure capturing and abort-on-failure behavior of reindex.
 */
public class ReindexFailureTests extends ReindexTestCase {
    public void testFailuresCauseAbortDefault() throws Exception {
        /*
         * Create the destination index such that the copy will cause a mapping
         * conflict on every request.
         */
        indexRandom(true,
                client().prepareIndex("dest", "test", "test").setSource("test", 10) /* Its a string in the source! */);

        indexDocs(100);

        ReindexRequestBuilder copy = reindex().source("source").destination("dest");
        /*
         * Set the search size to something very small to cause there to be
         * multiple batches for this request so we can assert that we abort on
         * the first batch.
         */
        copy.source().setSize(1);

        BulkIndexByScrollResponse response = copy.get();
        assertThat(response, matcher()
                .batches(1)
                .failures(both(greaterThan(0)).and(lessThanOrEqualTo(maximumNumberOfShards()))));
        for (Failure failure: response.getIndexingFailures()) {
            assertThat(failure.getMessage(), containsString("NumberFormatException[For input string: \"words words\"]"));
        }
    }

    public void testAbortOnVersionConflict() throws Exception {
        // Just put something in the way of the copy.
        indexRandom(true,
                client().prepareIndex("dest", "test", "1").setSource("test", "test"));

        indexDocs(100);

        ReindexRequestBuilder copy = reindex().source("source").destination("dest").abortOnVersionConflict(true);
        // CREATE will cause the conflict to prevent the write.
        copy.destination().setOpType(CREATE);

        BulkIndexByScrollResponse response = copy.get();
        assertThat(response, matcher().batches(1).versionConflicts(1).failures(1).created(99));
        for (Failure failure: response.getIndexingFailures()) {
            assertThat(failure.getMessage(), containsString("VersionConflictEngineException[[test]["));
        }
    }

    /**
     * Make sure that search failures get pushed back to the user as failures of
     * the whole process. We do lose some information about how far along the
     * process got, but its important that they see these failures.
     */
    public void testResponseOnSearchFailure() throws Exception {
        /*
         * Attempt to trigger a reindex failure by deleting the source index out
         * from under it.
         */
        int attempt = 1;
        while (attempt < 5) {
            indexDocs(100);
            ReindexRequestBuilder copy = reindex().source("source").destination("dest");
            copy.source().setSize(10);
            Future<BulkIndexByScrollResponse> response = copy.execute();
            client().admin().indices().prepareDelete("source").get();

            try {
                response.get();
                logger.info("Didn't trigger a reindex failure on the {} attempt", attempt);
                attempt++;
            } catch (ExecutionException e) {
                logger.info("Triggered a reindex failure on the {} attempt", attempt);
                assertThat(e.getMessage(), either(containsString("all shards failed")).or(containsString("No search context found")));
                return;
            }
        }
        assumeFalse("Wasn't able to trigger a reindex failure in " + attempt + " attempts.", true);
    }

    public void testSettingTtlIsValidationFailure() throws Exception {
        indexDocs(1);
        ReindexRequestBuilder copy = reindex().source("source").destination("dest");
        copy.destination().setTTL(123);
        try {
            copy.get();
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(), containsString("setting ttl on destination isn't supported. use scripts instead."));
        }
    }

    public void testSettingTimestampIsValidationFailure() throws Exception {
        indexDocs(1);
        ReindexRequestBuilder copy = reindex().source("source").destination("dest");
        copy.destination().setTimestamp("now");
        try {
            copy.get();
        } catch (ActionRequestValidationException e) {
            assertThat(e.getMessage(), containsString("setting timestamp on destination isn't supported. use scripts instead."));
        }
    }

    private void indexDocs(int count) throws Exception {
        List<IndexRequestBuilder> docs = new ArrayList<IndexRequestBuilder>(count);
        for (int i = 0; i < count; i++) {
            docs.add(client().prepareIndex("source", "test", Integer.toString(i)).setSource("test", "words words"));
        }
        indexRandom(true, docs);
    }
}
