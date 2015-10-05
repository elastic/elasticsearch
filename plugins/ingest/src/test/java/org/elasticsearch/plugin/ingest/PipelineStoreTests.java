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

package org.elasticsearch.plugin.ingest;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.ingest.SimpleProcessor;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipelineStoreTests extends ESTestCase {

    private PipelineStore store;
    private ThreadPool threadPool;
    private PipelineConfigDocReader docReader;

    @Before
    public void init() {
        threadPool = new ThreadPool("test");
        ClusterService clusterService = mock(ClusterService.class);
        docReader = mock(PipelineConfigDocReader.class);
        store = new PipelineStore(Settings.EMPTY, threadPool, clusterService, docReader, Collections.singletonMap(SimpleProcessor.TYPE, new SimpleProcessor.Builder.Factory()));
        store.start();
    }

    @After
    public void cleanup() {
        store.stop();
        threadPool.shutdown();
    }


    public void testUpdatePipeline() {
        List<SearchHit> hits = new ArrayList<>();
        hits.add(new InternalSearchHit(0, "1", new StringText("type"), Collections.emptyMap())
                .sourceRef(new BytesArray("{\"name\": \"_name1\", \"description\": \"_description1\"}"))
        );

        when(docReader.readAll()).thenReturn(hits);
        assertThat(store.get("1"), nullValue());

        store.updatePipelines();
        assertThat(store.get("1").getId(), equalTo("_name1"));
        assertThat(store.get("1").getDescription(), equalTo("_description1"));

        hits.add(new InternalSearchHit(0, "2", new StringText("type"), Collections.emptyMap())
                        .sourceRef(new BytesArray("{\"name\": \"_name2\", \"description\": \"_description2\"}"))
        );
        store.updatePipelines();
        assertThat(store.get("1").getId(), equalTo("_name1"));
        assertThat(store.get("1").getDescription(), equalTo("_description1"));
        assertThat(store.get("2").getId(), equalTo("_name2"));
        assertThat(store.get("2").getDescription(), equalTo("_description2"));
    }

    public void testPipelineUpdater() throws Exception {
        List<SearchHit> hits = new ArrayList<>();
        hits.add(new InternalSearchHit(0, "1", new StringText("type"), Collections.emptyMap())
                        .sourceRef(new BytesArray("{\"name\": \"_name1\", \"description\": \"_description1\"}"))
        );
        when(docReader.readAll()).thenReturn(hits);
        assertThat(store.get("1"), nullValue());

        store.startUpdateWorker();
        assertBusy(() -> {
            assertThat(store.get("1"), notNullValue());
            assertThat(store.get("1").getId(), equalTo("_name1"));
            assertThat(store.get("1").getDescription(), equalTo("_description1"));
        });

        hits.add(new InternalSearchHit(0, "2", new StringText("type"), Collections.emptyMap())
                        .sourceRef(new BytesArray("{\"name\": \"_name2\", \"description\": \"_description2\"}"))
        );
        assertBusy(() -> {
            assertThat(store.get("1"), notNullValue());
            assertThat(store.get("1").getId(), equalTo("_name1"));
            assertThat(store.get("1").getDescription(), equalTo("_description1"));
            assertThat(store.get("2"), notNullValue());
            assertThat(store.get("2").getId(), equalTo("_name2"));
            assertThat(store.get("2").getDescription(), equalTo("_description2"));
        });
    }

}
