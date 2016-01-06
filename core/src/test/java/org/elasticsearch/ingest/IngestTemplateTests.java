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

import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Before;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IngestTemplateTests extends ESSingleNodeTestCase {

    private IngestBootstrapper bootstrapper;

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    @Before
    public void init() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(Runnable::run);
        Environment environment = mock(Environment.class);
        ClusterService clusterService = mock(ClusterService.class);
        TransportService transportService = mock(TransportService.class);
        bootstrapper = new IngestBootstrapper(
            Settings.EMPTY, threadPool, environment, clusterService, transportService, new ProcessorsRegistry()
        );
        bootstrapper.setClient(client());
    }

    public void testInstallIndexTemplate() throws Exception {
        verifyNoIndexTemplates();
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", clusterState, clusterState));
        verifyIngestIndexTemplateExist();
    }

    public void testInstallTemplateAfterItHasBeenRemoved() throws Exception {
        verifyNoIndexTemplates();
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", clusterState, clusterState));
        verifyIngestIndexTemplateExist();

        client().admin().indices().prepareDeleteTemplate(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME).get();
        verifyNoIndexTemplates();

        clusterState = client().admin().cluster().prepareState().get().getState();
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", clusterState, clusterState));
        verifyIngestIndexTemplateExist();
    }

    public void testDoNotInstallTemplateBecauseIngestIndexTemplateAlreadyExists() throws Exception {
        // add an empty template and check that it doesn't get overwritten:
        client().admin().indices().preparePutTemplate(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME).setTemplate(".ingest").get();
        GetIndexTemplatesResponse  response = client().admin().indices().prepareGetTemplates(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME).get();
        assertThat(response.getIndexTemplates().size(), Matchers.equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getOrder(), Matchers.equalTo(0));

        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        bootstrapper.clusterChanged(new ClusterChangedEvent("test", clusterState, clusterState));

        response = client().admin().indices().prepareGetTemplates(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME).get();
        assertThat(response.getIndexTemplates().size(), Matchers.equalTo(1));
        assertThat("The empty index template shouldn't get overwritten", response.getIndexTemplates().get(0).getOrder(), Matchers.equalTo(0));
        assertThat("The empty index template shouldn't get overwritten", response.getIndexTemplates().get(0).getMappings().size(), Matchers.equalTo(0));
    }

    private static void verifyIngestIndexTemplateExist() {
        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME).get();
        assertThat(response.getIndexTemplates().size(), Matchers.equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getName(), Matchers.equalTo(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME));
        assertThat(response.getIndexTemplates().get(0).getOrder(), Matchers.equalTo(Integer.MAX_VALUE));
        assertThat(response.getIndexTemplates().get(0).getMappings().size(), Matchers.equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getMappings().get(PipelineStore.TYPE), Matchers.notNullValue());
    }

    private static void verifyNoIndexTemplates() {
        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
        assertThat(response.getIndexTemplates().size(), Matchers.equalTo(0));
    }

}
