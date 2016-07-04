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

package org.elasticsearch.action.support;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.equalTo;

public class AutoCreateIndexTests extends ESTestCase {

    public void testParseFailed() {
        try {
            new AutoCreateIndex(Settings.builder().put("action.auto_create_index", ",,,").build(), new IndexNameExpressionResolver(Settings.EMPTY));
            fail("initialization should have failed");
        } catch (IllegalArgumentException ex) {
            assertEquals("Can't parse [,,,] for setting [action.auto_create_index] must be either [true, false, or a comma separated list of index patterns]", ex.getMessage());
        }
    }

    public void testParseFailedMissingIndex() {
        String prefix = randomFrom("+", "-");
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), prefix).build();
        try {
            new AutoCreateIndex(settings, new IndexNameExpressionResolver(settings));
            fail("initialization should have failed");
        } catch(IllegalArgumentException ex) {
            assertEquals("Can't parse [" + prefix + "] for setting [action.auto_create_index] must contain an index name after [" + prefix + "]", ex.getMessage());
        }
    }

    public void testAutoCreationDisabled() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), false).build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(Settings.EMPTY));
        assertThat(autoCreateIndex.shouldAutoCreate(randomAsciiOfLengthBetween(1, 10), buildClusterState()), equalTo(false));
    }

    public void testAutoCreationEnabled() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), true).build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(Settings.EMPTY));
        assertThat(autoCreateIndex.shouldAutoCreate(randomAsciiOfLengthBetween(1, 10), buildClusterState()), equalTo(true));
    }

    public void testDefaultAutoCreation() {
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(Settings.EMPTY, new IndexNameExpressionResolver(Settings.EMPTY));
        assertThat(autoCreateIndex.shouldAutoCreate(randomAsciiOfLengthBetween(1, 10), buildClusterState()), equalTo(true));
    }

    public void testExistingIndex() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), randomFrom(true, false, randomAsciiOfLengthBetween(7, 10))).build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(Settings.EMPTY));
        assertThat(autoCreateIndex.shouldAutoCreate(randomFrom("index1", "index2", "index3"), buildClusterState("index1", "index2", "index3")), equalTo(false));
    }

    public void testDynamicMappingDisabled() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), randomFrom(true, randomAsciiOfLengthBetween(1, 10)))
            .put(MapperService.INDEX_MAPPER_DYNAMIC_SETTING.getKey(), false).build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(Settings.EMPTY));
        assertThat(autoCreateIndex.shouldAutoCreate(randomAsciiOfLengthBetween(1, 10), buildClusterState()), equalTo(false));
    }

    public void testAutoCreationPatternEnabled() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), randomFrom("+index*", "index*")).build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(settings));
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metaData(MetaData.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("index" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(true));
        assertThat(autoCreateIndex.shouldAutoCreate("does_not_match" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(false));
    }

    public void testAutoCreationPatternDisabled() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "-index*").build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(settings));
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metaData(MetaData.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("index" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(false));
        //default is false when patterns are specified
        assertThat(autoCreateIndex.shouldAutoCreate("does_not_match" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(false));
    }

    public void testAutoCreationMultiplePatternsWithWildcards() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), randomFrom("+test*,-index*", "test*,-index*")).build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(settings));
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metaData(MetaData.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("index" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(false));
        assertThat(autoCreateIndex.shouldAutoCreate("test" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(true));
        assertThat(autoCreateIndex.shouldAutoCreate("does_not_match" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(false));
    }

    public void testAutoCreationMultiplePatternsNoWildcards() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "+test1,-index1").build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(settings));
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metaData(MetaData.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("test1", clusterState), equalTo(true));
        assertThat(autoCreateIndex.shouldAutoCreate("index" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(false));
        assertThat(autoCreateIndex.shouldAutoCreate("test" + randomAsciiOfLengthBetween(2, 5), clusterState), equalTo(false));
        assertThat(autoCreateIndex.shouldAutoCreate("does_not_match" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(false));
    }

    public void testAutoCreationMultipleIndexNames() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "test1,test2").build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(settings));
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metaData(MetaData.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("test1", clusterState), equalTo(true));
        assertThat(autoCreateIndex.shouldAutoCreate("test2", clusterState), equalTo(true));
        assertThat(autoCreateIndex.shouldAutoCreate("does_not_match" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(false));
    }

    public void testAutoCreationConflictingPatternsFirstWins() {
        Settings settings = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), "+test1,-test1,-test2,+test2").build();
        AutoCreateIndex autoCreateIndex = new AutoCreateIndex(settings, new IndexNameExpressionResolver(settings));
        ClusterState clusterState = ClusterState.builder(new ClusterName("test")).metaData(MetaData.builder()).build();
        assertThat(autoCreateIndex.shouldAutoCreate("test1", clusterState), equalTo(true));
        assertThat(autoCreateIndex.shouldAutoCreate("test2", clusterState), equalTo(false));
        assertThat(autoCreateIndex.shouldAutoCreate("does_not_match" + randomAsciiOfLengthBetween(1, 5), clusterState), equalTo(false));
    }

    private static ClusterState buildClusterState(String... indices) {
        MetaData.Builder metaData = MetaData.builder();
        for (String index : indices) {
            metaData.put(IndexMetaData.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1));
        }
        return ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).build();
    }
}
