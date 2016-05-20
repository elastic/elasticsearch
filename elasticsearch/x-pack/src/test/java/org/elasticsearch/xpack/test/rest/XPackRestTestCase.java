/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;


import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.client.ElasticsearchResponseException;
import org.elasticsearch.common.bytes.BytesReference;
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
import org.elasticsearch.test.rest.client.RestTestResponse;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;


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
        try {
            getAdminExecutionContext().callApi("xpack.watcher.start", emptyMap(), emptyList(), emptyMap());
        } catch(ElasticsearchResponseException e) {
            //TODO ignore for now, needs to be fixed though
        }
    }

    @After
    public void stopWatcher() throws Exception {
        try {
            getAdminExecutionContext().callApi("xpack.watcher.stop", emptyMap(), emptyList(), emptyMap());
        } catch(ElasticsearchResponseException e) {
            //TODO ignore for now, needs to be fixed though
        }
    }

    @Before
    public void installLicense() throws Exception {
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        TestUtils.generateSignedLicense("trial", TimeValue.timeValueHours(2)).toXContent(builder, ToXContent.EMPTY_PARAMS);
        final BytesReference bytes = builder.bytes();
        try (XContentParser parser = XContentFactory.xContent(bytes).createParser(bytes)) {
            final List<Map<String, Object>> bodies = singletonList(singletonMap("license",
                    parser.map()));
            getAdminExecutionContext().callApi("license.post", singletonMap("acknowledge", "true"),
                    bodies, singletonMap("Authorization", BASIC_AUTH_VALUE));
        }
    }

    @After
    public void clearShieldUsersAndRoles() throws Exception {
        // we cannot delete the .security index from a rest test since we aren't the internal user, lets wipe the data
        // TODO remove this once the built-in SUPERUSER role is added that can delete the index and we use the built in admin user here
        RestTestResponse response = getAdminExecutionContext().callApi("xpack.security.get_user", emptyMap(), emptyList(), emptyMap());
        @SuppressWarnings("unchecked")
        Map<String, Object> users = (Map<String, Object>) response.getBody();
        for (String user: users.keySet()) {
            if (ReservedRealm.isReserved(user) == false) {
                getAdminExecutionContext().callApi("xpack.security.delete_user", singletonMap("username", user), emptyList(), emptyMap());
            }
        }

        response = getAdminExecutionContext().callApi("xpack.security.get_role", emptyMap(), emptyList(), emptyMap());
        @SuppressWarnings("unchecked")
        Map<String, Object> roles = (Map<String, Object>) response.getBody();
        for (String role: roles.keySet()) {
            if (ReservedRolesStore.isReserved(role) == false) {
                getAdminExecutionContext().callApi("xpack.security.delete_role", singletonMap("name", role), emptyList(), emptyMap());
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
