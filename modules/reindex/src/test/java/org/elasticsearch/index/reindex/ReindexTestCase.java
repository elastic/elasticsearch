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

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;

@ClusterScope(scope = SUITE, transportClientRatio = 0)
public abstract class ReindexTestCase extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(ReindexPlugin.class);
    }

    protected ReindexRequestBuilder reindex() {
        return ReindexAction.INSTANCE.newRequestBuilder(client());
    }

    protected UpdateByQueryRequestBuilder updateByQuery() {
        return UpdateByQueryAction.INSTANCE.newRequestBuilder(client());
    }

    protected DeleteByQueryRequestBuilder deleteByQuery() {
        return DeleteByQueryAction.INSTANCE.newRequestBuilder(client());
    }

    protected RethrottleRequestBuilder rethrottle() {
        return RethrottleAction.INSTANCE.newRequestBuilder(client());
    }

    public static BulkIndexByScrollResponseMatcher matcher() {
        return new BulkIndexByScrollResponseMatcher();
    }
}
