/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.example.realm;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * Integration test to test authentication with the custom realm
 */
public class CustomRealmIT extends ESRestTestCase {

    // These are configured in build.gradle
    public static final String USERNAME = "test_user";
    public static final String PASSWORD = "secret_password";

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
            .put(ThreadContext.PREFIX + "." + CustomRealm.USER_HEADER, USERNAME)
            .put(ThreadContext.PREFIX + "." + CustomRealm.PW_HEADER, PASSWORD)
            .build();
    }

    public void testHttpConnectionWithNoAuthentication() {
        Request request = new Request("GET", "/");
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader(CustomRealm.USER_HEADER, "");
        builder.addHeader(CustomRealm.PW_HEADER, "");
        request.setOptions(builder);
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
        Response response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), is(401));
        String value = response.getHeader("WWW-Authenticate");
        assertThat(value, is("custom-challenge"));
    }

    public void testHttpAuthentication() throws Exception {
        Request request = new Request("GET", "/");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        options.addHeader(CustomRealm.USER_HEADER, USERNAME);
        options.addHeader(CustomRealm.PW_HEADER, PASSWORD);
        request.setOptions(options);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testSettingsFiltering() throws Exception {
        Request request = new Request("GET", "/_nodes/_all/settings");
        request.addParameter("flat_settings", "true");
        Response response = client().performRequest(request);
        String responseString = EntityUtils.toString(response.getEntity());
        assertThat(responseString, not(containsString("xpack.security.authc.realms.custom.my_realm.filtered_setting")));
        assertThat(responseString, containsString("xpack.security.authc.realms.custom.my_realm.order"));
    }
}
