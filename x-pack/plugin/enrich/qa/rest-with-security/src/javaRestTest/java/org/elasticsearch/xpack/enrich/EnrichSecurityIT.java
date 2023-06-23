/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.enrich.CommonEnrichRestTestCase;

import static org.hamcrest.CoreMatchers.containsString;

public class EnrichSecurityIT extends CommonEnrichRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_enrich", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testInsufficientPermissionsOnNonExistentIndex() throws Exception {
        // This test is here because it requires a valid user that has permission to execute policy PUTs but should fail if the user
        // does not have access to read the backing indices used to enrich the data.
        Request request = new Request("PUT", "/some-other-index");
        request.setJsonEntity(Strings.format("""
            {
             "mappings" : {%s}
            }""", createSourceIndexMapping()));
        adminClient().performRequest(request);
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity(generatePolicySource("some-other-index"));
        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(putPolicyRequest));
        assertThat(
            exc.getMessage(),
            containsString("unable to store policy because no indices match with the specified index patterns [some-other-index]")
        );
    }
}
