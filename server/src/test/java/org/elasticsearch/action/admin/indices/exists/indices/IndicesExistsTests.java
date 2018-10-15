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

package org.elasticsearch.action.admin.indices.exists.indices;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolverTests;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

public class IndicesExistsTests extends ESTestCase {

    public void testEmptyWildCard() {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(Settings.EMPTY);
        IndicesExistsRequest request = new IndicesExistsRequest("*");
        ClusterState state = new ClusterState.Builder(new ClusterName("foo")).build();
        boolean exists = TransportIndicesExistsAction.doCheckIndicesExists(resolver, request, state);
        assertFalse(exists);
    }

    public void testWildCard() {
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver(Settings.EMPTY);
        IndicesExistsRequest request = new IndicesExistsRequest("test*");
        MetaData.Builder mdBuilder = MetaData.builder()
            .put(IndexNameExpressionResolverTests.indexBuilder("testXXX").state(IndexMetaData.State.OPEN))
            .put(IndexNameExpressionResolverTests.indexBuilder("testYYY").state(IndexMetaData.State.OPEN))
            .put(IndexNameExpressionResolverTests.indexBuilder("testZZZ").state(IndexMetaData.State.OPEN));
        ClusterState state = ClusterState.builder(new ClusterName("_name")).metaData(mdBuilder).build();
        boolean exists = TransportIndicesExistsAction.doCheckIndicesExists(resolver, request, state);
        assertTrue(exists);
    }
}
