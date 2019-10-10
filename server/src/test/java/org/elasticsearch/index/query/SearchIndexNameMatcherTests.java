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

package org.elasticsearch.index.query;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchIndexNameMatcherTests extends ESTestCase {
    private SearchIndexNameMatcher matcher;
    private SearchIndexNameMatcher remoteMatcher;

    @Before
    public void setUpMatchers() {
        MetaData.Builder metaDataBuilder = MetaData.builder()
            .put(indexBuilder("index1").putAlias(AliasMetaData.builder("alias")))
            .put(indexBuilder("index2").putAlias(AliasMetaData.builder("alias")))
            .put(indexBuilder("index3"));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(metaDataBuilder).build();

        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);

        matcher = new SearchIndexNameMatcher("index1", "", clusterService);
        remoteMatcher = new SearchIndexNameMatcher("index1", "cluster", clusterService);
    }

    private static IndexMetaData.Builder indexBuilder(String index) {
        Settings.Builder settings = settings(Version.CURRENT).
                put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0);
        return IndexMetaData.builder(index).settings(settings);
    }

    public void testLocalIndex() {
        assertTrue(matcher.test("index1"));
        assertTrue(matcher.test("ind*x1"));
        assertFalse(matcher.test("index2"));

        assertTrue(matcher.test("alias"));
        assertTrue(matcher.test("*lias"));

        assertFalse(matcher.test("cluster:index1"));
    }

    public void testRemoteIndex() {
        assertTrue(remoteMatcher.test("cluster:index1"));
        assertTrue(remoteMatcher.test("cluster:ind*x1"));
        assertTrue(remoteMatcher.test("*luster:ind*x1"));
        assertFalse(remoteMatcher.test("cluster:index2"));

        assertTrue(remoteMatcher.test("cluster:alias"));
        assertTrue(remoteMatcher.test("cluster:*lias"));

        assertFalse(remoteMatcher.test("index1"));
        assertFalse(remoteMatcher.test("alias"));

        assertFalse(remoteMatcher.test("*index1"));
        assertFalse(remoteMatcher.test("*alias"));
        assertFalse(remoteMatcher.test("cluster*"));
        assertFalse(remoteMatcher.test("cluster*index1"));
    }
}
