/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.wildfly;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@TestRuleLimitSysouts.Limit(bytes = 14000)
public class WildflyIT extends ESTestCase {

    private Logger logger = LogManager.getLogger(WildflyIT.class);

    private String buildBaseUrl() {
        final String propertyName = "test.fixtures.wildfly.tcp.8080";
        final String port = System.getProperty(propertyName);
        if (port == null) {
            throw new IllegalStateException(
                "Could not find system property "
                    + propertyName
                    + ". This test expects to run with the elasticsearch.test.fixtures Gradle plugin"
            );
        }
        return "http://localhost:" + port + "/example-app/transport";
    }

    public void testRestClient() throws URISyntaxException, IOException {
        final String baseUrl = buildBaseUrl();

        try (CloseableHttpClient client = HttpClientBuilder.create().build()) {
            final String endpoint = baseUrl + "/employees/1";
            logger.info("Connecting to uri: " + baseUrl);

            final HttpPut put = new HttpPut(new URI(endpoint));

            final String body = "{"
                + "  \"first_name\": \"John\","
                + "  \"last_name\": \"Smith\","
                + "  \"age\": 25,"
                + "  \"about\": \"I love to go rock climbing\","
                + "  \"interests\": ["
                + "    \"sports\","
                + "    \"music\""
                + "  ]"
                + "}";

            put.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
            try (CloseableHttpResponse response = client.execute(put)) {
                int status = response.getStatusLine().getStatusCode();
                assertThat(
                    "expected a 201 response but got: " + status + " - body: " + EntityUtils.toString(response.getEntity()),
                    status,
                    equalTo(201)
                );
            }

            logger.info("Fetching resource at " + endpoint);

            final HttpGet get = new HttpGet(new URI(endpoint));
            try (
                CloseableHttpResponse response = client.execute(get);
                XContentParser parser = JsonXContent.jsonXContent.createParser(
                    new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    response.getEntity().getContent()
                )
            ) {
                final Map<String, Object> map = parser.map();
                assertThat(map.get("first_name"), equalTo("John"));
                assertThat(map.get("last_name"), equalTo("Smith"));
                assertThat(map.get("age"), equalTo(25));
                assertThat(map.get("about"), equalTo("I love to go rock climbing"));
                final Object interests = map.get("interests");
                assertThat(interests, instanceOf(List.class));
                @SuppressWarnings("unchecked")
                final List<String> interestsAsList = (List<String>) interests;
                assertThat(interestsAsList, containsInAnyOrder("sports", "music"));
            }
        }
    }

}
