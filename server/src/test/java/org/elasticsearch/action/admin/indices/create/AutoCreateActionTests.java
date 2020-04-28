/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.AutoCreateAction.Request;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AutoCreateActionTests extends ESTestCase {

    private MetadataCreateIndexService mcis;
    private IndexNameExpressionResolver resolver;

    @Before
    public void setup() {
        mcis = mock(MetadataCreateIndexService.class);
        resolver = new IndexNameExpressionResolver();
    }

    public void testAutoCreate() throws Exception {
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        Request request = new Request(Set.of("index1", "index2", "index3"), "reason", randomBoolean() ? null : randomBoolean());
        expectApplyCreateIndexRequest(mcis, request, Set.of());

        Map<String, Exception> result = new HashMap<>();
        cs = AutoCreateAction.autoCreate(request, result, cs, mcis, resolver);
        assertThat(cs.metadata().indices().size(), equalTo(3));
        assertThat(cs.metadata().index("index1"), notNullValue());
        assertThat(cs.metadata().index("index2"), notNullValue());
        assertThat(cs.metadata().index("index3"), notNullValue());
        assertThat(result.size(), equalTo(3));
        assertThat(result.containsKey("index1"), is(true));
        assertThat(result.get("index1"), nullValue());
        assertThat(result.containsKey("index2"), is(true));
        assertThat(result.get("index2"), nullValue());
        assertThat(result.containsKey("index3"), is(true));
        assertThat(result.get("index3"), nullValue());

        verify(mcis, times(3)).applyCreateIndexRequest(any(ClusterState.class),
            any(CreateIndexClusterStateUpdateRequest.class), eq(false));
    }

    public void testAutoCreateIndexAlreadyExists() throws Exception {
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).metadata(Metadata.builder()
            .put(IndexMetadata.builder("index2")
                .settings(Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build())
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build(), false)).build();
        Request request = new Request(Set.of("index1", "index2", "index3"), "reason", randomBoolean() ? null : randomBoolean());
        expectApplyCreateIndexRequest(mcis, request, Set.of());

        Map<String, Exception> result = new HashMap<>();
        cs = AutoCreateAction.autoCreate(request, result, cs, mcis, resolver);
        assertThat(cs.metadata().indices().size(), equalTo(3));
        assertThat(cs.metadata().index("index1"), notNullValue());
        assertThat(cs.metadata().index("index2"), notNullValue());
        assertThat(cs.metadata().index("index3"), notNullValue());
        assertThat(result.size(), equalTo(2));
        assertThat(result.containsKey("index1"), is(true));
        assertThat(result.get("index1"), nullValue());
        assertThat(result.containsKey("index2"), is(false));
        assertThat(result.get("index2"), nullValue());
        assertThat(result.containsKey("index3"), is(true));
        assertThat(result.get("index3"), nullValue());

        verify(mcis, times(3)).applyCreateIndexRequest(any(ClusterState.class),
            any(CreateIndexClusterStateUpdateRequest.class), eq(false));
    }

    public void testAutoCreateFailure() throws Exception {
        ClusterState cs = ClusterState.builder(new ClusterName("_name")).build();
        Request request = new Request(Set.of("index1", "index2", "index3"), "reason", randomBoolean() ? null : randomBoolean());
        expectApplyCreateIndexRequest(mcis, request, Set.of("index2"));

        Map<String, Exception> result = new HashMap<>();
        cs = AutoCreateAction.autoCreate(request, result, cs, mcis, resolver);
        assertThat(cs.metadata().indices().size(), equalTo(2));
        assertThat(cs.metadata().index("index1"), notNullValue());
        assertThat(cs.metadata().index("index2"), nullValue());
        assertThat(cs.metadata().index("index3"), notNullValue());
        assertThat(result.size(), equalTo(3));
        assertThat(result.containsKey("index1"), is(true));
        assertThat(result.get("index1"), nullValue());
        assertThat(result.containsKey("index2"), is(true));
        assertThat(result.get("index2"), notNullValue());
        assertThat(result.get("index2").getMessage(), equalTo("fail!"));
        assertThat(result.containsKey("index3"), is(true));
        assertThat(result.get("index3"), nullValue());

        verify(mcis, times(3)).applyCreateIndexRequest(any(ClusterState.class),
            any(CreateIndexClusterStateUpdateRequest.class), eq(false));
    }

    private void expectApplyCreateIndexRequest(MetadataCreateIndexService mcis,
                                               Request request,
                                               Set<String> indicesToFail) throws Exception {
        when(mcis.applyCreateIndexRequest(any(ClusterState.class), any(CreateIndexClusterStateUpdateRequest.class), eq(false)))
            .thenAnswer(mockInvocation -> {
                ClusterState currentState = (ClusterState) mockInvocation.getArguments()[0];
                CreateIndexClusterStateUpdateRequest updateRequest =
                    (CreateIndexClusterStateUpdateRequest) mockInvocation.getArguments()[1];
                assertThat(updateRequest.index(), in(request.getNames()));
                assertThat(updateRequest.getProvidedName(), in(request.getNames()));
                assertThat(updateRequest.cause(), equalTo(request.getCause()));
                assertThat(updateRequest.masterNodeTimeout(), equalTo(request.masterNodeTimeout()));
                assertThat(updateRequest.preferV2Templates(), equalTo(request.getPreferV2Templates()));

                if (currentState.metadata().hasIndex(updateRequest.index())) {
                    throw new ResourceAlreadyExistsException(currentState.metadata().index(updateRequest.index()).getIndex());
                }

                if (indicesToFail.contains(updateRequest.index())) {
                    throw new Exception("fail!");
                }

                Metadata.Builder b = Metadata.builder(currentState.metadata())
                    .put(IndexMetadata.builder(updateRequest.index())
                        .settings(Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(updateRequest.settings())
                            .build())
                        .numberOfShards(1)
                        .numberOfReplicas(1)
                        .build(), false);
                return ClusterState.builder(currentState).metadata(b.build()).build();
            });
    }

}
