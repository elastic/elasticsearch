/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ObjectPath;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Map;

/**
 * Integration Rest Test for testing authentication when all possible realms are configured
 */
public class RealmInfoIT extends SecurityRealmSmokeTestCase {

    public void testThatAllRealmTypesAreEnabled() throws IOException {
        final Request request = new Request("GET", "_xpack/usage");
        final Response response = client().performRequest(request);
        Map<String, Object> usage = entityAsMap(response);

        Map<String, Object> realms = ObjectPath.evaluate(usage, "security.realms");
        realms.forEach((type, config) -> {
            assertThat(config, Matchers.instanceOf(Map.class));
            assertThat("Realm type [" + type + "] is not enabled", ((Map<?, ?>) config).get("enabled"), Matchers.equalTo(true));
        });
    }

}
