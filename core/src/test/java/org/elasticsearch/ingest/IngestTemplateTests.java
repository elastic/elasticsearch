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

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testIngestIndexTemplateIsInstalled() throws Exception {
        assertBusy(IngestTemplateTests::verifyIngestIndexTemplateExist);
    }

    public void testInstallTemplateAfterItHasBeenRemoved() throws Exception {
        assertBusy(IngestTemplateTests::verifyIngestIndexTemplateExist);
        client().admin().indices().prepareDeleteTemplate(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME).get();
        assertBusy(IngestTemplateTests::verifyIngestIndexTemplateExist);
    }

    private static void verifyIngestIndexTemplateExist() {
        GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME).get();
        assertThat(response.getIndexTemplates().size(), Matchers.equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getName(), Matchers.equalTo(IngestBootstrapper.INGEST_INDEX_TEMPLATE_NAME));
        assertThat(response.getIndexTemplates().get(0).getOrder(), Matchers.equalTo(Integer.MAX_VALUE));
        assertThat(response.getIndexTemplates().get(0).getMappings().size(), Matchers.equalTo(1));
        assertThat(response.getIndexTemplates().get(0).getMappings().get(PipelineStore.TYPE), Matchers.notNullValue());
    }

}
