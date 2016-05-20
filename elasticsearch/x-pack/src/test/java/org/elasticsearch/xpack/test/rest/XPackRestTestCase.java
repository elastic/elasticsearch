/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;


import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.ElasticsearchResponse;
import org.elasticsearch.client.ElasticsearchResponseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.shield.authc.esnative.ReservedRealm;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authz.store.ReservedRolesStore;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;


public abstract class XPackRestTestCase extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("test_user", new SecuredString("changeme".toCharArray()));

    public XPackRestTestCase(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ESRestTestCase.createParameters(0, 1);
    }

    @Before
    public void startWatcher() throws Exception {
        try (ElasticsearchResponse response = getRestClient()
                .performRequest("PUT", "/_xpack/watcher/_start", Collections.emptyMap(), null)) {
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }
    }

    @After
    public void stopWatcher() throws Exception {
        try (ElasticsearchResponse response = getRestClient()
                .performRequest("PUT", "/_xpack/watcher/_stop", Collections.emptyMap(), null)) {
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }
    }

    @Before
    public void installLicense() throws Exception {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        TestUtils.generateSignedLicense("trial", TimeValue.timeValueHours(2)).toXContent(builder, ToXContent.EMPTY_PARAMS);
        final BytesReference bytes = builder.bytes();
        try (XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes)) {
            final List<Map<String, Object>> bodies = Collections.singletonList(Collections.singletonMap("license",
                    parser.map()));
            getAdminExecutionContext().callApi("license.post", Collections.singletonMap("acknowledge", "true"),
                    bodies, Collections.singletonMap("Authorization", BASIC_AUTH_VALUE));
        }
    }

    @After
    public void clearShieldUsersAndRoles() throws Exception {
        // we cannot delete the .security index from a rest test since we aren't the internal user, lets wipe the data
        // TODO remove this once the built-in SUPERUSER role is added that can delete the index and we use the built in admin user here
        String response;
        try (ElasticsearchResponse elasticsearchResponse = getRestClient()
                .performRequest("GET", "/_xpack/security/user", Collections.emptyMap(), null)) {
            assertThat(elasticsearchResponse.getStatusLine().getStatusCode(), is(200));
            response = Streams.copyToString(new InputStreamReader(elasticsearchResponse.getEntity().getContent(), StandardCharsets.UTF_8));
        }

        Map<String, Object> responseMap = XContentFactory.xContent(response).createParser(response).map();
        // in the structure of this API, the users are the keyset
        for (String user : responseMap.keySet()) {
            if (ReservedRealm.isReserved(user) == false) {
                try (ElasticsearchResponse elasticsearchResponse = getRestClient()
                        .performRequest("DELETE", "/_xpack/security/user/" + user, Collections.emptyMap(), null)) {
                    assertThat(EntityUtils.toString(elasticsearchResponse.getEntity()), elasticsearchResponse.getStatusLine().getStatusCode(), is(200));
                } catch(ElasticsearchResponseException e) {
                    logger.error(e.getResponseBody());
                }
            }
        }

        try (ElasticsearchResponse elasticsearchResponse = getRestClient()
                .performRequest("GET", "/_xpack/security/role", Collections.emptyMap(), null)) {
            assertThat(elasticsearchResponse.getStatusLine().getStatusCode(), is(200));
            response = Streams.copyToString(new InputStreamReader(elasticsearchResponse.getEntity().getContent(), StandardCharsets.UTF_8));
        }
        responseMap = XContentFactory.xContent(response).createParser(response).map();
        // in the structure of this API, the roles are the keyset
        for (String role : responseMap.keySet()) {
            if (ReservedRolesStore.isReserved(role) == false) {
                try (ElasticsearchResponse elasticsearchResponse = getRestClient()
                        .performRequest("DELETE", "/_xpack/security/role/" + role, Collections.emptyMap(), null)) {
                    assertThat(elasticsearchResponse.getStatusLine().getStatusCode(), is(200));
                }
            }
        }
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
                .build();
    }
}
