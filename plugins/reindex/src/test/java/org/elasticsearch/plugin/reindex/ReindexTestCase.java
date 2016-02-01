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

package org.elasticsearch.plugin.reindex;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.util.Collection;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = SUITE, transportClientRatio = 0)
public abstract class ReindexTestCase extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(ReindexPlugin.class);
    }

    protected ReindexRequestBuilder reindex() {
        return ReindexAction.INSTANCE.newRequestBuilder(client());
    }

    public IndexBySearchResponseMatcher responseMatcher() {
        return new IndexBySearchResponseMatcher();
    }

    public static class IndexBySearchResponseMatcher
            extends AbstractBulkIndexByScrollResponseMatcher<ReindexResponse, IndexBySearchResponseMatcher> {
        private Matcher<Long> createdMatcher = equalTo(0L);

        public IndexBySearchResponseMatcher created(Matcher<Long> updatedMatcher) {
            this.createdMatcher = updatedMatcher;
            return this;
        }

        public IndexBySearchResponseMatcher created(long created) {
            return created(equalTo(created));
        }

        @Override
        protected boolean matchesSafely(ReindexResponse item) {
            return super.matchesSafely(item) && createdMatcher.matches(item.getCreated());
        }

        @Override
        public void describeTo(Description description) {
            super.describeTo(description);
            description.appendText(" and created matches ").appendDescriptionOf(createdMatcher);
        }

        @Override
        protected IndexBySearchResponseMatcher self() {
            return this;
        }
    }
}
