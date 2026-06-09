/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.qa;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Verifies that the entitlement agent emits its boot-time attach timing log line
 * during a normal node startup. Guards against silent loss of the operator-visible
 * diagnostic added for <a href="https://github.com/elastic/elasticsearch/issues/146333">#146333</a>.
 */
public class EntitlementAgentAttachLoggingIT extends ESRestTestCase {

    private static final Pattern ATTACH_LINE = Pattern.compile(
        "Entitlement agent attached in \\[(\\d+)ms\\] \\(attach=\\[(\\d+)ms\\], loadAgent\\+detach=\\[(\\d+)ms\\]\\)"
    );

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().setting("xpack.security.enabled", "false").build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testEntitlementAgentAttachLineIsLogged() throws IOException {
        client().performRequest(new Request("GET", "/_cluster/health"));

        try (
            InputStream log = cluster.getNodeLog(0, LogType.SERVER);
            BufferedReader reader = new BufferedReader(new InputStreamReader(log, StandardCharsets.UTF_8))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher m = ATTACH_LINE.matcher(line);
                if (m.find()) {
                    long total = Long.parseLong(m.group(1));
                    long attach = Long.parseLong(m.group(2));
                    long loadDetach = Long.parseLong(m.group(3));
                    assertThat(total, greaterThanOrEqualTo(attach + loadDetach));
                    return;
                }
            }
        }
        fail("entitlement agent attach timing log line was not found in node 0's log");
    }
}
