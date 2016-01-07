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

package org.elasticsearch.ingest;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentMatcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipelineStoreTests extends ESTestCase {

    private PipelineStore store;
    private Client client;

    @Before
    public void init() throws Exception {
        Settings settings = Settings.EMPTY;
        ClusterService clusterService = mock(ClusterService.class);
        TransportService transportService = mock(TransportService.class);

        client = mock(Client.class);
        when(client.search(any())).thenReturn(expectedSearchReponse(Collections.emptyList()));
        when(client.searchScroll(any())).thenReturn(expectedSearchReponse(Collections.emptyList()));
        store = new PipelineStore(settings, clusterService, transportService);
        store.setClient(client);
        store.start();
    }

    public void testUpdatePipeline() throws Exception {
        List<InternalSearchHit> hits = new ArrayList<>();
        hits.add(new InternalSearchHit(0, "1", new Text("type"), Collections.emptyMap())
                .sourceRef(new BytesArray("{\"description\": \"_description1\"}"))
        );

        when(client.search(any())).thenReturn(expectedSearchReponse(hits));
        when(client.get(any())).thenReturn(expectedGetResponse(true));
        assertThat(store.get("1"), nullValue());

        store.updatePipelines();
        assertThat(store.get("1").getId(), equalTo("1"));
        assertThat(store.get("1").getDescription(), equalTo("_description1"));

        when(client.get(any())).thenReturn(expectedGetResponse(true));
        hits.add(new InternalSearchHit(0, "2", new Text("type"), Collections.emptyMap())
                        .sourceRef(new BytesArray("{\"description\": \"_description2\"}"))
        );
        store.updatePipelines();
        assertThat(store.get("1").getId(), equalTo("1"));
        assertThat(store.get("1").getDescription(), equalTo("_description1"));
        assertThat(store.get("2").getId(), equalTo("2"));
        assertThat(store.get("2").getDescription(), equalTo("_description2"));

        hits.remove(1);
        when(client.get(eqGetRequest(PipelineStore.INDEX, PipelineStore.TYPE, "2"))).thenReturn(expectedGetResponse(false));
        store.updatePipelines();
        assertThat(store.get("1").getId(), equalTo("1"));
        assertThat(store.get("1").getDescription(), equalTo("_description1"));
        assertThat(store.get("2"), nullValue());
    }

    public void testGetReference() throws Exception {
        // fill the store up for the test:
        List<InternalSearchHit> hits = new ArrayList<>();
        hits.add(new InternalSearchHit(0, "foo", new Text("type"), Collections.emptyMap()).sourceRef(new BytesArray("{\"description\": \"_description\"}")));
        hits.add(new InternalSearchHit(0, "bar", new Text("type"), Collections.emptyMap()).sourceRef(new BytesArray("{\"description\": \"_description\"}")));
        hits.add(new InternalSearchHit(0, "foobar", new Text("type"), Collections.emptyMap()).sourceRef(new BytesArray("{\"description\": \"_description\"}")));
        when(client.search(any())).thenReturn(expectedSearchReponse(hits));
        store.updatePipelines();

        List<PipelineDefinition> result = store.getReference("foo");
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getPipeline().getId(), equalTo("foo"));

        result = store.getReference("foo*");
        // to make sure the order is consistent in the test:
        result.sort((first, second) -> {
            return first.getPipeline().getId().compareTo(second.getPipeline().getId());
        });
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0).getPipeline().getId(), equalTo("foo"));
        assertThat(result.get(1).getPipeline().getId(), equalTo("foobar"));

        result = store.getReference("bar*");
        assertThat(result.size(), equalTo(1));
        assertThat(result.get(0).getPipeline().getId(), equalTo("bar"));

        result = store.getReference("*");
        // to make sure the order is consistent in the test:
        result.sort((first, second) -> {
            return first.getPipeline().getId().compareTo(second.getPipeline().getId());
        });
        assertThat(result.size(), equalTo(3));
        assertThat(result.get(0).getPipeline().getId(), equalTo("bar"));
        assertThat(result.get(1).getPipeline().getId(), equalTo("foo"));
        assertThat(result.get(2).getPipeline().getId(), equalTo("foobar"));

        result = store.getReference("foo", "bar");
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0).getPipeline().getId(), equalTo("foo"));
        assertThat(result.get(1).getPipeline().getId(), equalTo("bar"));
    }

    public void testValidateIngestIndex() throws Exception {
        // ingest index doesn't exist:
        ClusterState state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder())
            .build();
        assertThat(store.isIngestIndexPresent(state), equalTo(false));

        // ingest index does exist and is valid:
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder(PipelineStore.INDEX)
            .settings(Settings.builder()
                .put(PipelineStore.INGEST_INDEX_SETTING)
                .put("index.version.created", Version.CURRENT)
            )
            .putMapping(PipelineStore.TYPE, PipelineStore.PIPELINE_MAPPING);
        state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetaData))
            .build();
        assertThat(store.isIngestIndexPresent(state), equalTo(true));

        // fails, has dynamic mapping
        indexMetaData = IndexMetaData.builder(PipelineStore.INDEX)
            .settings(Settings.builder()
                .put(PipelineStore.INGEST_INDEX_SETTING)
                .put("index.mapper.dynamic", true)
                .put("index.version.created", Version.CURRENT)
            )
            .putMapping(PipelineStore.TYPE, PipelineStore.PIPELINE_MAPPING);
        state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetaData))
            .build();
        try {
            store.isIngestIndexPresent(state);
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("illegal ingest index setting, [index.mapper.dynamic] setting is [true] while [false] is expected"));
        }

        // fails, incorrect number of primary shards
        indexMetaData = IndexMetaData.builder(PipelineStore.INDEX)
            .settings(Settings.builder()
                .put(PipelineStore.INGEST_INDEX_SETTING)
                .put("index.number_of_shards", 2)
                .put("index.version.created", Version.CURRENT)
            )
            .putMapping(PipelineStore.TYPE, PipelineStore.PIPELINE_MAPPING);
        state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetaData))
            .build();
        try {
            store.isIngestIndexPresent(state);
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("illegal ingest index setting, [index.number_of_shards] setting is [2] while [1] is expected"));
        }

        // fails, incorrect number of replica shards
        indexMetaData = IndexMetaData.builder(PipelineStore.INDEX)
            .settings(Settings.builder()
                .put(PipelineStore.INGEST_INDEX_SETTING)
                .put("index.number_of_replicas", 2)
                .put("index.version.created", Version.CURRENT)
            )
            .putMapping(PipelineStore.TYPE, PipelineStore.PIPELINE_MAPPING);
        state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetaData))
            .build();
        try {
            store.isIngestIndexPresent(state);
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("illegal ingest index setting, [index.number_of_replicas] setting is [2] while [1] is expected"));
        }

        // fails not a strict mapping:
        String mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("_all")
                .field("enabled", false)
            .endObject()
            .startObject("properties")
                .startObject("processors")
                    .field("type", "object")
                    .field("enabled", false)
                    .field("dynamic", true)
                .endObject()
                .startObject("on_failure")
                    .field("type", "object")
                    .field("enabled", false)
                    .field("dynamic", true)
                .endObject()
                .startObject("description")
                    .field("type", "string")
                .endObject()
            .endObject()
            .endObject().string();
        indexMetaData = IndexMetaData.builder(PipelineStore.INDEX)
            .settings(Settings.builder()
                .put(PipelineStore.INGEST_INDEX_SETTING)
                .put("index.version.created", Version.CURRENT)
            )
            .putMapping(PipelineStore.TYPE, mapping);
        state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetaData))
            .build();
        try {
            store.isIngestIndexPresent(state);
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("illegal ingest mapping, pipeline mapping must be strict"));
        }

        // fails _all field is enabled:
        mapping = XContentFactory.jsonBuilder().startObject()
            .field("dynamic", "strict")
            .startObject("_all")
                .field("enabled", true)
            .endObject()
            .startObject("properties")
                .startObject("processors")
                    .field("type", "object")
                    .field("enabled", false)
                    .field("dynamic", true)
                .endObject()
                .startObject("on_failure")
                    .field("type", "object")
                    .field("enabled", false)
                    .field("dynamic", true)
                .endObject()
                .startObject("description")
                    .field("type", "string")
                .endObject()
            .endObject()
            .endObject().string();
        indexMetaData = IndexMetaData.builder(PipelineStore.INDEX)
            .settings(Settings.builder()
                .put(PipelineStore.INGEST_INDEX_SETTING)
                .put("index.version.created", Version.CURRENT)
            )
            .putMapping(PipelineStore.TYPE, mapping);
        state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetaData))
            .build();
        try {
            store.isIngestIndexPresent(state);
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("illegal ingest mapping, _all field is enabled"));
        }

        // fails processor field not of type object:
        mapping = XContentFactory.jsonBuilder().startObject()
            .field("dynamic", "strict")
            .startObject("_all")
                .field("enabled", false)
            .endObject()
            .startObject("properties")
                .startObject("processors")
                    .field("type", "nested")
                    .field("enabled", false)
                    .field("dynamic", true)
                .endObject()
                .startObject("on_failure")
                    .field("type", "object")
                    .field("enabled", false)
                    .field("dynamic", true)
                .endObject()
                .startObject("description")
                    .field("type", "string")
                .endObject()
            .endObject()
            .endObject().string();
        indexMetaData = IndexMetaData.builder(PipelineStore.INDEX)
            .settings(Settings.builder()
                .put(PipelineStore.INGEST_INDEX_SETTING)
                .put("index.version.created", Version.CURRENT)
            )
            .putMapping(PipelineStore.TYPE, mapping);
        state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetaData))
            .build();
        try {
            store.isIngestIndexPresent(state);
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("illegal ingest mapping, processors field's type is [nested] while [object] is expected"));
        }

        // fails processor field enabled option is true:
        mapping = XContentFactory.jsonBuilder().startObject()
            .field("dynamic", "strict")
            .startObject("_all")
                .field("enabled", false)
            .endObject()
            .startObject("properties")
                .startObject("processors")
                    .field("type", "object")
                    .field("enabled", true)
                    .field("dynamic", true)
                .endObject()
                .startObject("on_failure")
                    .field("type", "object")
                    .field("enabled", false)
                    .field("dynamic", true)
                .endObject()
                .startObject("description")
                    .field("type", "string")
                .endObject()
            .endObject()
            .endObject().string();
        indexMetaData = IndexMetaData.builder(PipelineStore.INDEX)
            .settings(Settings.builder()
                .put(PipelineStore.INGEST_INDEX_SETTING)
                .put("index.version.created", Version.CURRENT)
            )
            .putMapping(PipelineStore.TYPE, mapping);
        state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetaData))
            .build();
        try {
            store.isIngestIndexPresent(state);
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("illegal ingest mapping, processors field enabled option is [true] while [false] is expected"));
        }

        // fails processor field dynamic option is false:
        mapping = XContentFactory.jsonBuilder().startObject()
            .field("dynamic", "strict")
            .startObject("_all")
                .field("enabled", false)
            .endObject()
            .startObject("properties")
                .startObject("processors")
                    .field("type", "object")
                    .field("enabled", false)
                    .field("dynamic", false)
                .endObject()
                .startObject("on_failure")
                    .field("type", "object")
                    .field("enabled", false)
                    .field("dynamic", true)
                .endObject()
                .startObject("description")
                    .field("type", "string")
                .endObject()
            .endObject()
            .endObject().string();
        indexMetaData = IndexMetaData.builder(PipelineStore.INDEX)
            .settings(Settings.builder()
                .put(PipelineStore.INGEST_INDEX_SETTING)
                .put("index.version.created", Version.CURRENT)
            )
            .putMapping(PipelineStore.TYPE, mapping);
        state = ClusterState.builder(new ClusterName("_name"))
            .metaData(MetaData.builder().put(indexMetaData))
            .build();
        try {
            store.isIngestIndexPresent(state);
            fail("exception expected");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("illegal ingest mapping, processors field dynamic option is [false] while [true] is expected"));
        }
    }

    static ActionFuture<SearchResponse> expectedSearchReponse(List<InternalSearchHit> hits) {
        return new PlainActionFuture<SearchResponse>() {

            @Override
            public SearchResponse get(long timeout, TimeUnit unit) {
                InternalSearchHits hits1 = new InternalSearchHits(hits.toArray(new InternalSearchHit[0]), hits.size(), 1f);
                return new SearchResponse(new InternalSearchResponse(hits1, null, null, null, false, null), "_scrollId", 1, 1, 1, null);
            }
        };
    }

    static ActionFuture<GetResponse> expectedGetResponse(boolean exists) {
        return new PlainActionFuture<GetResponse>() {
            @Override
            public GetResponse get() throws InterruptedException, ExecutionException {
                return new GetResponse(new GetResult("_index", "_type", "_id", 1, exists, null, null));
            }
        };
    }

    static GetRequest eqGetRequest(String index, String type, String id) {
        return argThat(new GetRequestMatcher(index, type, id));
    }

    static class GetRequestMatcher extends ArgumentMatcher<GetRequest> {

        private final String index;
        private final String type;
        private final String id;

        public GetRequestMatcher(String index, String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
        }

        @Override
        public boolean matches(Object o) {
            GetRequest getRequest = (GetRequest) o;
            return Objects.equals(getRequest.index(), index) &&
                    Objects.equals(getRequest.type(), type) &&
                    Objects.equals(getRequest.id(), id);
        }
    }

}
