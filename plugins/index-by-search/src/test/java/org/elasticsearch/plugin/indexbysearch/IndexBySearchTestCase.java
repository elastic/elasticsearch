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
import static org.hamcrest.Matchers.equalTo;

import java.util.Collection;

import org.elasticsearch.action.indexbysearch.IndexBySearchAction;
import org.elasticsearch.action.indexbysearch.IndexBySearchRequestBuilder;
import org.elasticsearch.action.indexbysearch.IndexBySearchResponse;
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

    public IndexBySearchResponseMatcher responseMatcher() {
        return new IndexBySearchResponseMatcher();
    }

    public class IndexBySearchResponseMatcher extends TypeSafeMatcher<IndexBySearchResponse> {
        private Matcher<Long> indexedMatcher = equalTo(0l);
        private Matcher<Long> createdMatcher = equalTo(0l);

        public IndexBySearchResponseMatcher indexed(Matcher<Long> indexedMatcher) {
            this.indexedMatcher = indexedMatcher;
            return this;
        }

        public IndexBySearchResponseMatcher indexed(long indexed) {
            return indexed(equalTo(indexed));
        }

        public IndexBySearchResponseMatcher created(Matcher<Long> createdMatcher) {
            this.createdMatcher = createdMatcher;
            return this;
        }

        public IndexBySearchResponseMatcher created(long created) {
            return created(equalTo(created));
        }

        @Override
        protected boolean matchesSafely(IndexBySearchResponse item) {
            if (indexedMatcher != null && indexedMatcher.matches(item.indexed()) == false) {
                return false;
            }
            if (createdMatcher != null && createdMatcher.matches(item.created()) == false) {
                return false;
            }
            return true;
        }

        @Override
        public void describeTo(Description description) {
            boolean started = false;
            if (indexedMatcher != null) {
                description.appendText("indexed matches ").appendDescriptionOf(indexedMatcher);
            }
            if (createdMatcher != null) {
                if (started) {
                    description.appendText(" and ");
                }
                description.appendText("created matches ").appendDescriptionOf(createdMatcher);
            }
        }
    }
}
