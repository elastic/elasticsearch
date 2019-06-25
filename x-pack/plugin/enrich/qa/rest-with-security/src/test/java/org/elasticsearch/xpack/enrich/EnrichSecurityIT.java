/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.enrich.CommonEnrichRestTestCase;

import java.io.IOException;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public class EnrichSecurityIT extends CommonEnrichRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_enrich", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    public void testImmutablePolicy() throws IOException {
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity("{\"type\": \"exact_match\",\"indices\": [\"my-source-index\"], \"enrich_key\": \"host\", " +
            "\"enrich_values\": [\"globalRank\", \"tldRank\", \"tld\"]}");
        assertOK(client().performRequest(putPolicyRequest));

        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(putPolicyRequest));
        assertTrue(exc.getMessage().contains("policy [my_policy] already exists"));
    }
}
