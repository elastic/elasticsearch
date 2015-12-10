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

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;

import java.util.Collection;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

@ClusterScope(scope = SUITE, transportClientRatio = 0)
public class IndexBySearchTestCase extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(IndexBySearchPlugin.class);
    }

    protected IndexBySearchRequestBuilder newIndexBySearch() {
        return IndexBySearchAction.INSTANCE.newRequestBuilder(client());
    }

    protected ReindexInPlaceRequestBuilder newReindex() {
        return ReindexInPlaceAction.INSTANCE.newRequestBuilder(client());
    }

    public IndexBySearchResponseMatcher responseMatcher() {
        return new IndexBySearchResponseMatcher();
    }

    public static class IndexBySearchResponseMatcher extends TypeSafeMatcher<IndexByScrollResponse> {
        private Matcher<Long> updatedMatcher = equalTo(0l);
        private Matcher<Long> createdMatcher = equalTo(0l);
        private Matcher<Integer> batchesMatcher = any(Integer.class);
        private Matcher<Long> versionConflictsMatcher = equalTo(0l);
        private Matcher<Integer> failuresMatcher = equalTo(0);

        public IndexBySearchResponseMatcher updated(Matcher<Long> updatedMatcher) {
            this.updatedMatcher = updatedMatcher;
            return this;
        }

        public IndexBySearchResponseMatcher updated(long updated) {
            return updated(equalTo(updated));
        }

        public IndexBySearchResponseMatcher created(Matcher<Long> updatedMatcher) {
            this.createdMatcher = updatedMatcher;
            return this;
        }

        public IndexBySearchResponseMatcher created(long created) {
            return created(equalTo(created));
        }

        /**
         * Set the matches for the number of batches. Defaults to matching any
         * integer because we usually don't care about how many batches the job
         * takes.
         */
        public IndexBySearchResponseMatcher batches(Matcher<Integer> batchesMatcher) {
            this.batchesMatcher = batchesMatcher;
            return this;
        }

        public IndexBySearchResponseMatcher batches(int batches) {
            return batches(equalTo(batches));
        }

        public IndexBySearchResponseMatcher versionConflicts(Matcher<Long> versionConflictsMatcher) {
            this.versionConflictsMatcher = versionConflictsMatcher;
            return this;
        }

        public IndexBySearchResponseMatcher versionConflicts(long versionConflicts) {
            return versionConflicts(equalTo(versionConflicts));
        }

        /**
         * Set the matcher for the size of the failures list. For more in depth
         * matching do it by hand. The type signatures required to match the
         * actual failures list here just don't work.
         */
        public IndexBySearchResponseMatcher failures(Matcher<Integer> failuresMatcher) {
            this.failuresMatcher = failuresMatcher;
            return this;
        }

        /**
         * Set the expected size of the failures list.
         */
        public IndexBySearchResponseMatcher failures(int failures) {
            return failures(equalTo(failures));
        }


        @Override
        protected boolean matchesSafely(IndexByScrollResponse item) {
            return updatedMatcher.matches(item.updated()) &&
                createdMatcher.matches(item.created()) &&
                batchesMatcher.matches(item.batches()) &&
                versionConflictsMatcher.matches(item.versionConflicts()) &&
                failuresMatcher.matches(item.failures().size());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("indexed matches ").appendDescriptionOf(updatedMatcher);
            description.appendText(" and created matches ").appendDescriptionOf(createdMatcher);
            description.appendText(" and batches matches ").appendDescriptionOf(batchesMatcher);
            description.appendText(" and versionConflicts matches ").appendDescriptionOf(versionConflictsMatcher);
            description.appendText(" and failures size matches ").appendDescriptionOf(failuresMatcher);
        }
    }
}
