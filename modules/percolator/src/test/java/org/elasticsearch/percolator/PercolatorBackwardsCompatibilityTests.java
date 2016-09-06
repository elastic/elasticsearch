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
package org.elasticsearch.percolator;

import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.scriptQuery;
import static org.elasticsearch.percolator.PercolatorTestUtil.preparePercolate;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
@LuceneTestCase.SuppressFileSystems("ExtrasFS")
// Can'r run as IT as the test cluster is immutable and this test adds nodes during the test
public class PercolatorBackwardsCompatibilityTests extends ESIntegTestCase {

    private static final String INDEX_NAME = "percolator_index";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(PercolatorPlugin.class, FoolMeScriptLang.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(PercolatorPlugin.class);
    }

    public void testOldPercolatorIndex() throws Exception {
        setupNode();

        // verify cluster state:
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertThat(state.metaData().indices().size(), equalTo(1));
        assertThat(state.metaData().indices().get(INDEX_NAME), notNullValue());
        assertThat(state.metaData().indices().get(INDEX_NAME).getCreationVersion(), equalTo(Version.V_2_0_0));
        assertThat(state.metaData().indices().get(INDEX_NAME).getUpgradedVersion(), equalTo(Version.CURRENT));
        assertThat(state.metaData().indices().get(INDEX_NAME).getMappings().size(), equalTo(2));
        assertThat(state.metaData().indices().get(INDEX_NAME).getMappings().get(".percolator"), notNullValue());
        // important: verify that the query field in the .percolator mapping is of type object (from 5.x this is of type percolator)
        MappingMetaData mappingMetaData = state.metaData().indices().get(INDEX_NAME).getMappings().get(".percolator");
        assertThat(XContentMapValues.extractValue("properties.query.type", mappingMetaData.sourceAsMap()), equalTo("object"));
        assertThat(state.metaData().indices().get(INDEX_NAME).getMappings().get("message"), notNullValue());

        // verify existing percolator queries:
        SearchResponse searchResponse = client().prepareSearch(INDEX_NAME)
            .setTypes(".percolator")
            .addSort("_uid", SortOrder.ASC)
            .get();
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(4L));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).id(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).id(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(3).id(), equalTo("4"));
        assertThat(XContentMapValues.extractValue("query.script.script.inline",
                searchResponse.getHits().getAt(3).sourceAsMap()), equalTo("return true"));
        // we don't upgrade the script definitions so that they include explicitly the lang,
        // because we read / parse the query at search time.
        assertThat(XContentMapValues.extractValue("query.script.script.lang",
                searchResponse.getHits().getAt(3).sourceAsMap()), nullValue());

        // verify percolate response
        PercolateResponse percolateResponse = preparePercolate(client())
                .setIndices(INDEX_NAME)
                .setDocumentType("message")
                .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("{}"))
                .get();

        assertThat(percolateResponse.getCount(), equalTo(1L));
        assertThat(percolateResponse.getMatches().length, equalTo(1));
        assertThat(percolateResponse.getMatches()[0].getId().string(), equalTo("4"));

        percolateResponse = preparePercolate(client())
            .setIndices(INDEX_NAME)
            .setDocumentType("message")
            .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("message", "the quick brown fox jumps over the lazy dog"))
            .get();

        assertThat(percolateResponse.getCount(), equalTo(3L));
        assertThat(percolateResponse.getMatches().length, equalTo(3));
        assertThat(percolateResponse.getMatches()[0].getId().string(), equalTo("1"));
        assertThat(percolateResponse.getMatches()[1].getId().string(), equalTo("2"));
        assertThat(percolateResponse.getMatches()[2].getId().string(), equalTo("4"));

        // add an extra query and verify the results
        client().prepareIndex(INDEX_NAME, ".percolator", "5")
            .setSource(jsonBuilder().startObject().field("query", matchQuery("message", "fox jumps")).endObject())
            .get();
        refresh();

        percolateResponse = preparePercolate(client())
            .setIndices(INDEX_NAME)
            .setDocumentType("message")
            .setPercolateDoc(new PercolateSourceBuilder.DocBuilder().setDoc("message", "the quick brown fox jumps over the lazy dog"))
            .get();

        assertThat(percolateResponse.getCount(), equalTo(4L));
        assertThat(percolateResponse.getMatches().length, equalTo(4));
        assertThat(percolateResponse.getMatches()[0].getId().string(), equalTo("1"));
        assertThat(percolateResponse.getMatches()[1].getId().string(), equalTo("2"));
        assertThat(percolateResponse.getMatches()[2].getId().string(), equalTo("4"));
    }

    private void setupNode() throws Exception {
        Path dataDir = createTempDir();
        Path clusterDir = Files.createDirectory(dataDir.resolve(cluster().getClusterName()));
        try (InputStream stream = PercolatorBackwardsCompatibilityTests.class.
                getResourceAsStream("/indices/percolator/bwc_index_2.0.0.zip")) {
            TestUtil.unzip(stream, clusterDir);
        }

        Settings.Builder nodeSettings = Settings.builder()
            .put(Environment.PATH_DATA_SETTING.getKey(), dataDir);
        internalCluster().startNode(nodeSettings.build());
        ensureGreen(INDEX_NAME);
    }

    // Fool the script service that this is the groovy script language,
    // so that we can run a script that has no lang defined implicetely against the legacy language:
    public static class FoolMeScriptLang extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            return Collections.singletonMap("return true", (vars) -> true);
        }

        @Override
        public String pluginScriptLang() {
            return "groovy";
        }
    }

}
