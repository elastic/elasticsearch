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
package org.elasticsearch.bwcompat;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.CustomPrototypeRegistry;
import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.env.Environment;
import org.elasticsearch.ingest.IngestMetadata;
import org.elasticsearch.ingest.PipelineConfiguration;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.ScriptMetaData;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
public class CustomClusterStatePartsBackwardsCompatibilityIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder setting = Settings.builder();
        setting.put(super.nodeSettings(nodeOrdinal));
        setting.put("repositories.url.allowed_urls", "http://snapshot.test*");
        return setting.build();
    }

    public void testUpgradeFrom5dot0() throws Exception {
        setupNode();
        ClusterState state = client().admin().cluster().prepareState().all().get().getState();
        ScriptMetaData scriptMetaData = state.getMetaData().custom(ScriptMetaData.TYPE);
        assertThat(scriptMetaData.getScript("painless", "script1"), equalTo("1 + 1"));
        assertThat(scriptMetaData.getScript("painless", "script2"), equalTo("1 + 2"));

        RepositoriesMetaData repositoriesMetaData = state.getMetaData().custom(RepositoriesMetaData.TYPE);
        RepositoryMetaData repo = repositoriesMetaData.repository("repo1");
        assertThat(repo.type(), equalTo("url"));
        assertThat(repo.settings().get("url"), equalTo("http://snapshot.test"));
        repo = repositoriesMetaData.repository("repo2");
        assertThat(repo.type(), equalTo("url"));
        assertThat(repo.settings().get("url"), equalTo("http://snapshot.test"));

        IngestMetadata ingestMetadata = state.getMetaData().custom(IngestMetadata.TYPE);
        PipelineConfiguration pipelineConfiguration = ingestMetadata.getPipelines().get("pipeline1");
        assertThat(pipelineConfiguration.getId(), equalTo("pipeline1"));
        assertThat(pipelineConfiguration.getConfigAsMap(), equalTo(Collections.singletonMap("processors", new ArrayList<>())));
        pipelineConfiguration = ingestMetadata.getPipelines().get("pipeline2");
        assertThat(pipelineConfiguration.getId(), equalTo("pipeline2"));
        assertThat(pipelineConfiguration.getConfigAsMap(), equalTo(Collections.singletonMap("processors", new ArrayList<>())));
    }

    private void setupNode() throws Exception {
        Path unzipDir = createTempDir();
        try (InputStream stream = CustomClusterStatePartsBackwardsCompatibilityIT.class.
            getResourceAsStream("/indices/bwc/custom-cluster-state-parts-5.0.0.zip")) {
            TestUtil.unzip(stream, unzipDir);
        }

        Settings.Builder nodeSettings = Settings.builder()
            .put(Environment.PATH_DATA_SETTING.getKey(), unzipDir.resolve("data"));
        internalCluster().startNode(nodeSettings.build());
        ensureYellow();
    }

}
