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

    public void testInsufficientPermissionsOnNonExistentIndex() {
        // This test is here because it requires a valid user that has permission to execute policy PUTs but should fail if the user
        // does not have access to read the backing indices used to enrich the data.
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity("{\"type\": \"exact_match\",\"indices\": [\"some-other-index\"], \"enrich_key\": \"host\", " +
            "\"enrich_values\": [\"globalRank\", \"tldRank\", \"tld\"]}");
        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(putPolicyRequest));
        assertTrue(exc.getMessage().contains("Could not store policy because an index specified [some-other-index] did not exist or user" +
            " test_enrich lacks permission to access it."));
    }
}
