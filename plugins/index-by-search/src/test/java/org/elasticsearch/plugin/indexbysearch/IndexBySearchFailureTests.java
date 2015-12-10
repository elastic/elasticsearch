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

package org.elasticsearch.plugin.indexbysearch;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.bulk.BulkItemResponse.Failure;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.index.VersionType;

/**
 * Tests failure capturing and abort-on-failure behavior of index-by-search.
 */
public class IndexBySearchFailureTests extends IndexBySearchTestCase {
    public void testFailuresCauseAbortDefault() throws Exception {
        /*
         * Create the destination index such that the copy will cause a mapping
         * conflict on every request.
         */
        indexRandom(true,
                client().prepareIndex("dest", "test", "test").setSource("test", 10) /* Its a string in the source! */);

        indexDocs(100);

        IndexBySearchRequestBuilder copy = newIndexBySearch();
        copy.search().setIndices("source");
        copy.index().setIndex("dest");
        /*
         * Set the search size to something very small to cause there to be
         * multiple batches for this request so we can assert that we abort on
         * the first batch.
         */
        copy.search().setSize(1);

        IndexBySearchResponse response = copy.get();
        assertThat(response, responseMatcher()
                .batches(1)
                .failures(both(greaterThan(0)).and(lessThanOrEqualTo(maximumNumberOfShards()))));
        for (Failure failure: response.failures()) {
            assertThat(failure.getMessage(), containsString("NumberFormatException[For input string: \"words words\"]"));
        }
    }

    public void testVersionConflictsRecorded() throws Exception {
        indexDocs(100);

        IndexBySearchRequestBuilder copy = newIndexBySearch().abortOnVersionConflict(true);
        copy.search().setIndices("source");
        copy.index().setIndex("dest");
        /*
         * Use internal versioning to cause a version conflict. The target is
         * empty and internal versioning will refuse to create the document.
         */
        copy.index().setVersionType(VersionType.INTERNAL);

        IndexBySearchResponse response = copy.get();
        assertThat(response, responseMatcher().batches(1).versionConflicts(greaterThan(0l)).failures(greaterThan(0)));
        for (Failure failure: response.failures()) {
            assertThat(failure.getMessage(), containsString("VersionConflictEngineException[[test]["));
        }
    }


    private void indexDocs(int count) throws Exception {
        List<IndexRequestBuilder> docs = new ArrayList<IndexRequestBuilder>(count);
        for (int i = 0; i < count; i++) {
            docs.add(client().prepareIndex("source", "test", Integer.toString(1)).setSource("test", "words words"));
        }
        indexRandom(true, docs);
    }
}
