/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;

public abstract class DataStreamLicenseChangeIT extends LogsIndexModeRestTestIT {
    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .module("data-streams")
        .module("x-pack-stack")
        .setting("cluster.logsdb.enabled", "true")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "basic")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    protected interface TestCase {
        String dataStreamName();

        void prepareDataStream() throws IOException;

        String indexMode();

        SourceFieldMapper.Mode initialMode();

        SourceFieldMapper.Mode finalMode();

        void rollover() throws IOException;
    }

    protected abstract void licenseChange() throws IOException;

    protected abstract void applyInitialLicense() throws IOException;

    protected abstract List<TestCase> cases();

    public void testLicenseChange() throws IOException {
        applyInitialLicense();

        for (var testCase : cases()) {
            testCase.prepareDataStream();

            var indexMode = (String) getSetting(client(), getDataStreamBackingIndex(client(), testCase.dataStreamName(), 0), "index.mode");
            assertEquals(testCase.indexMode(), indexMode);

            var sourceMode = (String) getSetting(
                client(),
                getDataStreamBackingIndex(client(), testCase.dataStreamName(), 0),
                "index.mapping.source.mode"
            );
            assertEquals(testCase.initialMode().toString(), sourceMode);
        }

        licenseChange();

        for (var testCase : cases()) {
            testCase.rollover();

            var indexMode = (String) getSetting(client(), getDataStreamBackingIndex(client(), testCase.dataStreamName(), 1), "index.mode");
            assertEquals(testCase.indexMode(), indexMode);

            var sourceMode = (String) getSetting(
                client(),
                getDataStreamBackingIndex(client(), testCase.dataStreamName(), 1),
                "index.mapping.source.mode"
            );
            assertEquals(testCase.finalMode().toString(), sourceMode);
        }
    }

    protected static void startBasic() throws IOException {
        Request startTrial = new Request("POST", "/_license/start_basic");
        startTrial.addParameter("acknowledge", "true");
        assertOK(client().performRequest(startTrial));
    }

    protected static void startTrial() throws IOException {
        Request startTrial = new Request("POST", "/_license/start_trial");
        startTrial.addParameter("acknowledge", "true");
        assertOK(client().performRequest(startTrial));
    }

    protected static Response removeComponentTemplate(final RestClient client, final String componentTemplate) throws IOException {
        final Request request = new Request("DELETE", "/_component_template/" + componentTemplate);
        return client.performRequest(request);
    }
}
