/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class LogsdbWithSecurityRestIT extends ESRestTestCase {

    private static final String PASSWORD = "secret-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("logsdb.usage_check.max_period", "1s")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.http.ssl.enabled", "false")
        .user("test_admin", PASSWORD, "superuser", true)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString(PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testPriorLogsUsage() throws Exception {
        {
            var getClusterSettingsRequest = new Request("GET", "/_cluster/settings");
            getClusterSettingsRequest.addParameter("flat_settings", "true");
            var getClusterSettingResponse = (Map<?, ?>) entityAsMap(client().performRequest(getClusterSettingsRequest));
            var persistentSettings = (Map<?, ?>) getClusterSettingResponse.get("persistent");
            assertThat(persistentSettings.get("logsdb.prior_logs_usage"), nullValue());
        }

        var request = new Request("POST", "/logs-test-foo/_doc");
        request.setJsonEntity("""
            {
                "@timestamp": "2020-01-01T00:00:00.000Z",
                "host.name": "foo",
                "message": "bar"
            }
            """);
        assertOK(client().performRequest(request));

        String index = DataStream.getDefaultBackingIndexName("logs-test-foo", 1);
        var settings = (Map<?, ?>) ((Map<?, ?>) getIndexSettings(index).get(index)).get("settings");
        assertNull(settings.get("index.mode"));
        assertNull(settings.get("index.mapping.source.mode"));

        assertBusy(() -> {
            var getClusterSettingsRequest = new Request("GET", "/_cluster/settings");
            getClusterSettingsRequest.addParameter("flat_settings", "true");
            var getClusterSettingResponse = (Map<?, ?>) entityAsMap(client().performRequest(getClusterSettingsRequest));
            var persistentSettings = (Map<?, ?>) getClusterSettingResponse.get("persistent");
            assertThat(persistentSettings.get("logsdb.prior_logs_usage"), equalTo("true"));
        });
    }

}
