/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.kibana.upgrade.test;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;

import static org.elasticsearch.test.cluster.local.distribution.DistributionType.DEFAULT;

public class DeprecatedDateFormatIndexMigrationIT extends ESRestTestCase {
    private static final Logger logger = LogManager.getLogger(DeprecatedDateFormatIndexMigrationIT.class);

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DEFAULT)
        .version(Version.fromString("7.17.28"))
        .setting("xpack.security.enabled", "false")
        .build();

    public void testDeprecation() throws InterruptedException, IOException {
        var mapping = """
            "properties": {
                "date": {
                  "type": "date",
                  "format": "qqqq yyyy"
                }
              }""";
        createIndex("old-index", Settings.EMPTY, mapping);
        indexDocument("old-index");
        cluster.upgradeToVersion(Version.CURRENT);
        startKibana();

        Thread.currentThread().join();
    }

    private void indexDocument(String index) throws IOException {
        Request indexRequest = new Request("POST", "/" + index + "/" + "_doc/");
        indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("date", "1 2025").endObject()));
        assertOK(client().performRequest(indexRequest));
    }

    private static void startKibana() {
        new GenericContainer<>("docker.elastic.co/kibana/kibana:8.18.0-SNAPSHOT")
            .withEnv("ELASTICSEARCH_HOSTS", "[\"http://" + cluster.getHttpAddresses() + "\"]")
            .withNetworkMode("host")
            .start();
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
