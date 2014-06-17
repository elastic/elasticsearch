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

package org.elasticsearch.search.scroll;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.action.search.type.ParsedScrollId;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;

/**
 */
@LuceneTestCase.Slow
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class SlowSearchScrollTests extends SearchScrollTests {

    private final Version[] versions = new Version[]{
            Version.CURRENT, ParsedScrollId.SCROLL_SEARCH_AFTER_MINIMUM_VERSION, Version.V_1_1_0, Version.V_1_0_0
    };

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        // If we add a constructor to InternalNode that allows us to define a version, then in the InternalTestCluster
        // we can start nodes with different versions and then we don't need this setting and would also be helpful
        // for other tests
        Settings settings =  super.nodeSettings(nodeOrdinal);
        Version randomVersion = versions[randomInt(versions.length - 1)];
        return ImmutableSettings.builder().put(settings).put("tests.mock.version", randomVersion.id).build();
    }
}
