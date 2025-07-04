/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package geoip;

import fixture.geoip.GeoIpHttpFixture;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.ingest.geoip.GeoIpDownloader;
import org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class GeoIpMultiProjectIT extends ESRestTestCase {
    // default true
    private static final boolean useFixture = Booleans.parseBoolean(System.getProperty("geoip_use_service", "false")) == false;

    public static final GeoIpHttpFixture fixture = new GeoIpHttpFixture(useFixture);

    public static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("ingest-geoip")
        .module("reindex")  // for database cleanup
        .module("test-multi-project")
        .setting("test.multi_project.enabled", "true")
        .setting(GeoIpDownloaderTaskExecutor.ENABLED_SETTING.getKey(), "true")
        .setting(GeoIpDownloader.ENDPOINT_SETTING.getKey(), fixture::getAddress, (k) -> useFixture)
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(fixture).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean shouldConfigureProjects() {
        return false;
    }

    public void testGeoIpDownloader() throws Exception {
        String project1 = randomUniqueProjectId().id();
        String project2 = randomUniqueProjectId().id();
        createProject(project1);
        createProject(project2);

        // download databases for project1
        putGeoIpPipeline(project1);
        assertBusy(() -> assertDatabases(project1, true), 30, TimeUnit.SECONDS);
        assertBusy(() -> assertDatabases(project2, false), 30, TimeUnit.SECONDS);

        // download databases for project2
        putGeoIpPipeline(project2);
        assertBusy(() -> assertDatabases(project2, true), 30, TimeUnit.SECONDS);
    }

    private void putGeoIpPipeline(String projectId) throws IOException {
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/geoip-pipeline");
        putPipelineRequest.setJsonEntity("""
            {
              "processors" : [
                {
                  "geoip" : {
                    "field" : "ip",
                    "target_field" : "geo",
                    "database_file" : "GeoLite2-Country.mmdb"
                  }
                }
              ]
            }
            """);
        setRequestProjectId(projectId, putPipelineRequest);
        assertOK(client().performRequest(putPipelineRequest));
    }

    private static Request setRequestProjectId(String projectId, Request request) {
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.removeHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER);
        options.addHeader(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, projectId);
        request.setOptions(options);
        return request;
    }

    @SuppressWarnings("unchecked")
    private void assertDatabases(String projectId, boolean shouldDownload) throws IOException {
        Request getTaskState = new Request("GET", "/_cluster/state");
        setRequestProjectId(projectId, getTaskState);

        ObjectPath state = ObjectPath.createFromResponse(assertOK(client().performRequest(getTaskState)));

        List<Map<String, ?>> tasks = state.evaluate("metadata.persistent_tasks.tasks");
        // Short-circuit to avoid using steams if the list is empty
        if (tasks.isEmpty()) {
            fail("persistent tasks list is empty, expected at least one task for geoip-downloader");
        }

        // verify project task id
        Set<Map<String, ?>> id = tasks.stream()
            .filter(task -> String.format("%s/geoip-downloader", projectId).equals(task.get("id")))
            .collect(Collectors.toSet());
        assertThat(id.size(), equalTo(1));

        // verify database download
        Map<String, Object> databases = (Map<String, Object>) tasks.stream().map(task -> {
            try {
                return ObjectPath.evaluate(task, "task.geoip-downloader.state.databases");
            } catch (IOException e) {
                return null;
            }
        }).filter(Objects::nonNull).findFirst().orElse(null);

        if (shouldDownload) {
            // verify database downloaded
            assertNotNull(databases);
            for (String name : List.of("GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb")) {
                Object database = databases.get(name);
                assertNotNull(database);
                assertNotNull(ObjectPath.evaluate(database, "md5"));
            }
        } else {
            // verify database not downloaded
            assertNull(databases);
        }

    }
}
