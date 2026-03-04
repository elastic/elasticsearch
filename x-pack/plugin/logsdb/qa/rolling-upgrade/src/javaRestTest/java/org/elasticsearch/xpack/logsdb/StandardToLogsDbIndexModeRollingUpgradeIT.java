/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

public class StandardToLogsDbIndexModeRollingUpgradeIT extends AbstractStringTypeLogsdbRollingUpgradeTestCase {

    private static final String USER = "admin-user";
    private static final String PASS = "x-pack-test-password";

    private static final String DATA_STREAM_NAME_PREFIX = "logs-standard-to-logs-bwc-test";

    private static final String TEMPLATE = """
        {
            "mappings": {
              "properties": {
                "@timestamp" : {
                  "type": "date"
                },
                "length": {
                  "type": "long"
                },
                "factor": {
                  "type": "double"
                },
                "message": {
                  "type": "text"
                }
              }
            }
        }""";

    @ClassRule
    public static final ElasticsearchCluster cluster = Clusters.oldVersionClusterWithLogsDisabled(
        USER,
        PASS,
        () -> initTestSeed().nextBoolean()
    );

    @Override
    protected ElasticsearchCluster getCluster() {
        return cluster;
    }

    @Override
    protected List<TemplateConfig> getTemplates() {
        return List.of(new TemplateConfig(DATA_STREAM_NAME_PREFIX + ".basic", TEMPLATE));
    }

    @Override
    public void testIndexing() throws Exception {
        List<TemplateConfig> templates = getTemplates();

        // before upgrading
        for (TemplateConfig config : templates) {
            indexDocumentsAndVerifyResults(config);
        }

        // verification must happen after we've indexed at least one document as streams are initialized lazily
        verifyIndexMode(IndexMode.STANDARD, templates.get(0).dataStreamName());

        // during upgrade
        for (int i = 0; i < getNumNodes(); i++) {
            upgradeNode(i);
            for (TemplateConfig config : templates) {
                indexDocumentsAndVerifyResults(config);
            }
        }

        enableLogsDb();
        for (TemplateConfig config : templates) {
            rolloverDataStream(config.dataStreamName());
        }
        verifyIndexMode(IndexMode.LOGSDB, templates.get(0).dataStreamName());

        // after everything is upgraded
        for (TemplateConfig config : templates) {
            indexDocumentsAndVerifyResults(config);
        }
    }

    private void enableLogsDb() throws IOException {
        // enable logsdb cluster setting
        var request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity("""
            {
                "persistent": {
                    "cluster.logsdb.enabled": true
                }
            }
            """);
        assertOK(client().performRequest(request));
    }

    private void rolloverDataStream(String dataStreamName) throws IOException {
        var request = new Request("POST", "/" + dataStreamName + "/_rollover");
        final Response response = client().performRequest(request);
        assertOK(response);
    }
}
