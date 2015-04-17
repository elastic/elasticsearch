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

import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.AbstractSnapshotTests;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.elasticsearch.test.ElasticsearchIntegrationTest.Scope;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.SortedSet;
import java.util.TreeSet;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.*;

@Slow
@ClusterScope(scope = Scope.TEST)
public class RestoreBackwardsCompatTests extends AbstractSnapshotTests {


    @Test
    public void restoreOldSnapshots() throws Exception {
        String repo = "test_repo";
        String snapshot = "test_1";
        List<String> repoVersions = repoVersions();
        assertThat(repoVersions.size(), greaterThan(0));
        for (String version : repoVersions) {
            createRepo(version, repo);
            testOldSnapshot(version, repo, snapshot);
        }

        SortedSet<String> expectedVersions = new TreeSet<>();
        for (java.lang.reflect.Field field : Version.class.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers()) && field.getType() == Version.class) {
                Version v = (Version) field.get(Version.class);
                if (v.snapshot()) continue;
                if (v.onOrBefore(Version.V_1_0_0_Beta1)) continue;
                if (v.equals(Version.CURRENT)) continue;

                expectedVersions.add(v.toString());
            }
        }

        for (String repoVersion : repoVersions) {
            if (expectedVersions.remove(repoVersion) == false) {
                logger.warn("Old repositories tests contain extra repo: " + repoVersion);
            }
        }
        if (expectedVersions.isEmpty() == false) {
            StringBuilder msg = new StringBuilder("Old repositories tests are missing versions:");
            for (String expected : expectedVersions) {
                msg.append("\n" + expected);
            }
            fail(msg.toString());
        }
    }

    private List<String> repoVersions() throws Exception {
        List<String> repoVersions = newArrayList();
        Path repoFiles = getDataPath(".");
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(repoFiles, "repo-*.zip")) {
            for (Path entry : stream) {
                String fileName = entry.getFileName().toString();
                String version = fileName.substring("repo-".length());
                version = version.substring(0, version.length() - ".zip".length());
                repoVersions.add(version);
            }
        }
        return repoVersions;
    }

    private void createRepo(String version, String repo) throws Exception {
        String repoFile = "repo-" + version + ".zip";
        URI repoFileUri = getClass().getResource(repoFile).toURI();
        URI repoJarUri = new URI("jar:" + repoFileUri.toString() + "!/repo/");
        logger.info("-->  creating repository [{}] for version [{}]", repo, version);
        assertAcked(client().admin().cluster().preparePutRepository(repo)
                .setType("url").setSettings(ImmutableSettings.settingsBuilder()
                        .put("url", repoJarUri.toString())));
    }

    private void testOldSnapshot(String version, String repo, String snapshot) throws IOException {
        logger.info("--> restoring snapshot");
        RestoreSnapshotResponse response = client().admin().cluster().prepareRestoreSnapshot(repo, snapshot).setRestoreGlobalState(true).setWaitForCompletion(true).get();
        assertThat(response.status(), equalTo(RestStatus.OK));
        RestoreInfo restoreInfo = response.getRestoreInfo();
        assertThat(restoreInfo.successfulShards(), greaterThan(0));
        assertThat(restoreInfo.successfulShards(), equalTo(restoreInfo.totalShards()));
        assertThat(restoreInfo.failedShards(), equalTo(0));
        String index = restoreInfo.indices().get(0);

        logger.info("--> check search");
        SearchResponse searchResponse = client().prepareSearch(index).get();
        assertThat(searchResponse.getHits().totalHits(), greaterThan(1L));

        logger.info("--> check settings");
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        assertThat(clusterState.metaData().persistentSettings().get(FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP + "version_attr"), equalTo(version));

        logger.info("--> check templates");
        IndexTemplateMetaData template = clusterState.getMetaData().templates().get("template_" + version.toLowerCase(Locale.ROOT));
        assertThat(template, notNullValue());
        assertThat(template.template(), equalTo("te*"));
        assertThat(template.settings().getAsInt(IndexMetaData.SETTING_NUMBER_OF_SHARDS, -1), equalTo(1));
        assertThat(template.mappings().size(), equalTo(1));
        assertThat(template.mappings().get("type1").string(), equalTo("{\"type1\":{\"_source\":{\"enabled\":false}}}"));
        if (Version.fromString(version).onOrAfter(Version.V_1_1_0)) {
            // Support for aliases in templates was added in v1.1.0
            assertThat(template.aliases().size(), equalTo(3));
            assertThat(template.aliases().get("alias1"), notNullValue());
            assertThat(template.aliases().get("alias2").filter().string(), containsString(version));
            assertThat(template.aliases().get("alias2").indexRouting(), equalTo("kimchy"));
            assertThat(template.aliases().get("{index}-alias"), notNullValue());
        }

        logger.info("--> cleanup");
        cluster().wipeIndices(restoreInfo.indices().toArray(new String[restoreInfo.indices().size()]));
        cluster().wipeTemplates();

    }
}

