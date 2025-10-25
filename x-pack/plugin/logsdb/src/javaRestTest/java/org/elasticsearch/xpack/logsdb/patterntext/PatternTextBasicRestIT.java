/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;
import org.junit.ClassRule;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class PatternTextBasicRestIT extends ESRestTestCase {

    private static final String USER = "test_admin";
    private static final String PASS = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.autoconfiguration.enabled", "false")
        .user(USER, PASS)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @ParametersFactory(argumentFormatting = "disableTemplating=%b")
    public static List<Object[]> args() {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    private final boolean disableTemplating;

    public PatternTextBasicRestIT(boolean disableTemplating) {
        this.disableTemplating = disableTemplating;
    }

    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(USER, new SecureString(PASS.toCharArray()));
        return Settings.builder().put(super.restClientSettings()).put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @SuppressWarnings("unchecked")
    public void testBulkInsertThenMatchAllSource() throws IOException {

        Settings.Builder settings = Settings.builder();
        if (randomBoolean()) {
            settings.put("index.mode", "logsdb");
        }
        if (randomBoolean()) {
            settings.put("index.mapping.source.mode", "synthetic");
        }

        String mapping = """
                {
                    "properties": {
                        "@timestamp": {
                            "type": "date"
                        },
                        "message": {
                            "type": "pattern_text",
                            "disable_templating": %disable_templating%
                        }
                    }
                }
            """.replace("%disable_templating%", Boolean.toString(disableTemplating));

        String indexName = "test-index";
        createIndex(indexName, settings.build(), mapping);

        int numDocs = randomIntBetween(1, 100);
        List<String> messages = randomMessages(numDocs);
        indexDocs(indexName, messages);

        var actualMapping = getIndexMappingAsMap(indexName);
        assertThat("pattern_text", equalTo(ObjectPath.evaluate(actualMapping, "properties.message.type")));

        Request searchRequest = new Request("GET", "/" + indexName + "/_search");
        searchRequest.setJsonEntity("""
            {
                "query" : { "match_all" : {} },
                "size": 100
            }
            """);
        Response getResponse = client().performRequest(searchRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        ObjectPath objectPath = ObjectPath.createFromResponse(getResponse);

        assertThat(objectPath.evaluate("hits.total.value"), equalTo(messages.size()));
        var hits = (ArrayList<Map<String, Object>>) objectPath.evaluate("hits.hits");
        var values = new HashSet<>(hits.stream().map(o -> {
            var source = (Map<String, Object>) o.get("_source");
            return (String) source.get("message");
        }).toList());

        assertEquals(new HashSet<>(messages), values);
    }

    private void indexDocs(String indexName, List<String> messages) throws IOException {
        var now = Instant.now();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < messages.size(); i++) {
            sb.append("{ \"create\": {} }").append('\n');
            if (messages.get(i) == null) {
                sb.append("""
                        {"@timestamp": "$now"}
                    """.replace("$now", formatInstant(now)));
            } else {
                sb.append("""
                    {"@timestamp": "$now", "message": "$msg"}
                    """.replace("$now", formatInstant(now)).replace("$msg", messages.get(i)));
            }
            sb.append('\n');
            now = now.plusSeconds(1);
        }

        var bulkRequest = new Request("POST", "/" + indexName + "/_bulk");
        bulkRequest.setJsonEntity(sb.toString());
        bulkRequest.addParameter("refresh", "true");
        var bulkResponse = client().performRequest(bulkRequest);
        var bulkResponseBody = responseAsMap(bulkResponse);
        assertThat(bulkResponseBody, Matchers.hasEntry("errors", false));
    }

    static String formatInstant(Instant instant) {
        return DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(instant);
    }

    public static List<String> randomMessages(int numDocs) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String message = randomFrom(random(), () -> null, () -> randomMessage(10), () -> randomMessage(8 * 1024));
            messages.add(message);
        }
        return messages;
    }

    static String randomMessage(int minLength) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < minLength) {
            var token = randomBoolean() ? randomAlphaOfLength(randomIntBetween(1, 10)) : randomInt();
            sb.append(token).append(" ");
        }
        return sb.toString();
    }
}
