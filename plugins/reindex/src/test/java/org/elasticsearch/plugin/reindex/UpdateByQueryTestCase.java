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

import java.util.Collection;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

@ClusterScope(scope = SUITE, transportClientRatio = 0)
public abstract class UpdateByQueryTestCase extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(ReindexPlugin.class);
    }

    protected UpdateByQueryRequestBuilder request() {
        return UpdateByQueryAction.INSTANCE.newRequestBuilder(client());
    }

    public BulkIndexbyScrollResponseMatcher responseMatcher() {
        return new BulkIndexbyScrollResponseMatcher();
    }

    public static class BulkIndexbyScrollResponseMatcher extends
            AbstractBulkIndexByScrollResponseMatcher<BulkIndexByScrollResponse, BulkIndexbyScrollResponseMatcher> {
        @Override
        protected BulkIndexbyScrollResponseMatcher self() {
            return this;
        }
    }
}
