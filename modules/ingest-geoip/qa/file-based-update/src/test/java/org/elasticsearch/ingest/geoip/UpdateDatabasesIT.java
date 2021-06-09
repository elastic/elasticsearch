/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.geoip;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class UpdateDatabasesIT extends ESRestTestCase {

    public void test() throws Exception {
        String body = "{\"pipeline\":{\"processors\":[{\"geoip\":{\"field\":\"ip\"}}]}," +
            "\"docs\":[{\"_index\":\"index\",\"_id\":\"id\",\"_source\":{\"ip\":\"89.160.20.128\"}}]}";
        Request simulatePipelineRequest = new Request("POST", "/_ingest/pipeline/_simulate");
        simulatePipelineRequest.setJsonEntity(body);
        {
            Map<String, Object> response = toMap(client().performRequest(simulatePipelineRequest));
            assertThat(ObjectPath.eval("docs.0.doc._source.geoip.city_name", response), equalTo("Tumba"));
        }

        Path configPath = PathUtils.get(System.getProperty("tests.config.dir"));
        assertThat(Files.exists(configPath), is(true));
        Path ingestGeoipDatabaseDir = configPath.resolve("ingest-geoip");
        Files.createDirectory(ingestGeoipDatabaseDir);
        Files.copy(UpdateDatabasesIT.class.getResourceAsStream("/GeoLite2-City-Test.mmdb"),
            ingestGeoipDatabaseDir.resolve("GeoLite2-City.mmdb"));

        assertBusy(() -> {
            Map<String, Object> response = toMap(client().performRequest(simulatePipelineRequest));
            assertThat(ObjectPath.eval("docs.0.doc._source.geoip.city_name", response), equalTo("Linköping"));
        });
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

}
