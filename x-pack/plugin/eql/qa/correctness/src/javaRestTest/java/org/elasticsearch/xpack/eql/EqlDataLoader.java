/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.logging.log4j.core.config.plugins.util.PluginManager;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;

import static org.elasticsearch.test.ESTestCase.assertEquals;

@SuppressWarnings("removal")
public class EqlDataLoader {

    private static final String PROPERTIES_FILENAME = "config.properties";

    public static void main(String[] args) throws IOException {
        // Need to setup the log configuration properly to avoid messages when creating a new RestClient
        PluginManager.addPackage(LogConfigurator.class.getPackage().getName());
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("admin", "admin-password"));
        try (
            RestClient client = RestClient.builder(new HttpHost("localhost", 9200))
                .setHttpClientConfigCallback(new HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                })
                .setRequestConfigCallback(
                    requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(30000000)
                        .setConnectionRequestTimeout(30000000)
                        .setSocketTimeout(30000000)
                )
                .setStrictDeprecationMode(true)
                .build()
        ) {
            Properties configuration = loadConfiguration();
            restoreSnapshot(new RestHighLevelClient(client, ignore -> {}, List.of()) {
            }, configuration);
        }
    }

    static Properties loadConfiguration() throws IOException {
        try (InputStream is = EqlDataLoader.class.getClassLoader().getResourceAsStream(PROPERTIES_FILENAME)) {
            Properties props = new Properties();
            props.load(is);
            return props;
        }
    }

    static void restoreSnapshot(RestHighLevelClient restHighLevelClient, Properties cfg) throws IOException {
        if (restHighLevelClient.getLowLevelClient()
            .performRequest(new Request("HEAD", "/" + cfg.getProperty("index_name")))
            .getStatusLine()
            .getStatusCode() == 404) {
            restHighLevelClient.snapshot()
                .createRepository(
                    new PutRepositoryRequest(cfg.getProperty("gcs_repo_name")).type("gcs")
                        .settings(
                            Settings.builder()
                                .put("bucket", cfg.getProperty("gcs_bucket_name"))
                                .put("base_path", cfg.getProperty("gcs_base_path"))
                                .put("client", cfg.getProperty("gcs_client_name"))
                                .build()
                        ),
                    RequestOptions.DEFAULT
                );
            RestoreSnapshotResponse resp = restHighLevelClient.snapshot()
                .restore(
                    new RestoreSnapshotRequest(cfg.getProperty("gcs_repo_name"), cfg.getProperty("gcs_snapshot_name")).waitForCompletion(
                        true
                    ),
                    RequestOptions.DEFAULT
                );

            assertEquals(
                "Unable to restore snapshot: "
                    + resp.getRestoreInfo().toString()
                    + System.lineSeparator()
                    + "Please check server logs to find the underlying issue.",
                1,
                resp.getRestoreInfo().successfulShards()
            );

            assertEquals(
                Long.parseLong(cfg.getProperty("index_doc_count")),
                restHighLevelClient.count(new CountRequest(cfg.getProperty("index_name")), RequestOptions.DEFAULT).getCount()
            );
        }
    }
}
