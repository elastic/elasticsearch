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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

public class AutoCreateIndexTests extends ESTestCase {

    public void testBasic() {
        {
            AutoCreateIndex autoCreateIndex = new AutoCreateIndex(Settings.EMPTY, new IndexNameExpressionResolver(Settings.EMPTY));
            ClusterState cs = buildClusterState("foo");
            assertFalse("already exists", autoCreateIndex.shouldAutoCreate("foo", cs));
            assertTrue(autoCreateIndex.shouldAutoCreate("foobar", cs));
        }
        {
            AutoCreateIndex autoCreateIndex = new AutoCreateIndex(Settings.builder().put("action.auto_create_index", "-foo,+b*").build(), new IndexNameExpressionResolver(Settings.EMPTY));
            ClusterState cs = buildClusterState("foobar", "baz");
            assertFalse(autoCreateIndex.shouldAutoCreate("foo", cs));
            assertTrue(autoCreateIndex.shouldAutoCreate("bar", cs));
            assertFalse("already exists", autoCreateIndex.shouldAutoCreate("baz", cs));
        }

        {
            AutoCreateIndex autoCreateIndex = new AutoCreateIndex(Settings.builder().put("action.auto_create_index", "-foo,+b*").put("index.mapper.dynamic", false).build(), new IndexNameExpressionResolver(Settings.EMPTY));
            ClusterState cs = buildClusterState("foobar", "baz");
            assertFalse(autoCreateIndex.shouldAutoCreate("foo", cs));
            assertFalse(autoCreateIndex.shouldAutoCreate("bar", cs));
            assertFalse("already exists", autoCreateIndex.shouldAutoCreate("baz", cs));
        }

        {
            AutoCreateIndex autoCreateIndex = new AutoCreateIndex(Settings.builder().put("action.auto_create_index", false).put("index.mapper.dynamic", false).build(), new IndexNameExpressionResolver(Settings.EMPTY));
            ClusterState cs = buildClusterState("foobar", "baz");
            assertFalse(autoCreateIndex.shouldAutoCreate("foo", cs));
            assertFalse(autoCreateIndex.shouldAutoCreate("bar", cs));
            assertFalse("already exists", autoCreateIndex.shouldAutoCreate("baz", cs));
        }
    }

    public void testParseFailed() {
        try {
            new AutoCreateIndex(Settings.builder().put("action.auto_create_index", ",,,").build(), new IndexNameExpressionResolver(Settings.EMPTY));
        }catch (IllegalArgumentException ex) {
            assertEquals("Can't parse [,,,] for setting [action.auto_create_index] must be either [true, false, or a comma seperated list of index patterns]", ex.getMessage());
        }

    }

    public ClusterState buildClusterState(String... indices) {
        MetaData.Builder metaData = MetaData.builder();
        for (String index : indices) {
            metaData.put(IndexMetaData.builder(index).settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1));
        }
        return ClusterState.builder(org.elasticsearch.cluster.ClusterName.DEFAULT).metaData(metaData).build();
    }
}
