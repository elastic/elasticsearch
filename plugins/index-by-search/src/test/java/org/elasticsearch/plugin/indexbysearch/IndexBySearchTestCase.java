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

import org.elasticsearch.action.indexbysearch.IndexBySearchAction;
import org.elasticsearch.action.indexbysearch.IndexBySearchRequestBuilder;
import org.elasticsearch.action.indexbysearch.IndexBySearchResponse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Collection;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = SUITE, transportClientRatio = 0) // Required to use plugins?
public class IndexBySearchTestCase extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(IndexBySearchPlugin.class);
    }

    protected IndexBySearchRequestBuilder newIndexBySearch() {
        return IndexBySearchAction.INSTANCE.newRequestBuilder(client());
    }

    protected void assertResponse(IndexBySearchResponse response, long expectedIndexed, long expectedCreated) {
        assertThat(response.indexed(), equalTo(expectedIndexed));
    }
}
