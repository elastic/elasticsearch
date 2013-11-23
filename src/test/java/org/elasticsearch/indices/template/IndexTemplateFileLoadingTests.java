/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.elasticsearch.indices.template;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.hamcrest.Matchers.is;

/**
 *
 */
@ClusterScope(scope= Scope.SUITE, numNodes=1)
public class IndexTemplateFileLoadingTests extends ElasticsearchIntegrationTest {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        ImmutableSettings.Builder settingsBuilder = ImmutableSettings.settingsBuilder();
        settingsBuilder.put(super.nodeSettings(nodeOrdinal));

        try {
            File directory = temporaryFolder.newFolder();
            settingsBuilder.put("path.conf", directory.getPath());

            File templatesDir = new File(directory + File.separator + "templates");
            templatesDir.mkdir();

            File dst = new File(templatesDir, "template.json");
            String template = Streams.copyToStringFromClasspath("/org/elasticsearch/indices/template/template.json");
            Files.write(template, dst, Charsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return settingsBuilder.build();
    }

    @Test
    public void testThatLoadingTemplateFromFileWorks() throws Exception {
        createIndex("foobar");
        ensureYellow(); // ensuring yellow so the test fails faster if the template cannot be loaded

        ClusterStateResponse stateResponse = client().admin().cluster().prepareState().setFilterIndices("foobar").get();
        assertThat(stateResponse.getState().getMetaData().indices().get("foobar").getNumberOfShards(), is(10));
        assertThat(stateResponse.getState().getMetaData().indices().get("foobar").getNumberOfReplicas(), is(0));
    }
}
