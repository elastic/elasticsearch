/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.audit.logfile;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = TEST, numDataNodes = 1)
public class AuditRequestBodyLimitIT extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put("xpack.security.audit.enabled", "true")
            .put(LoggingAuditTrail.INCLUDE_REQUEST_BODY.getKey(), "true")
            .put(LoggingAuditTrail.MAX_REQUEST_BODY_SIZE.getKey(), "10b")
            .putList(LoggingAuditTrail.INCLUDE_EVENT_SETTINGS.getKey(), "authentication_success")
            .build();
    }

    public void testRequestBodyExceedingLimitReturns413() throws Exception {
        String body = "{\"field\":\"this value is well over ten bytes long\"}";
        Request request = new Request("PUT", "/test-index/_doc/1");
        request.setJsonEntity(body);
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(
                SecuritySettingsSource.ES_TEST_ROOT_USER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
            )
        );
        request.setOptions(options);

        ResponseException ex = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), is(413));
        assertThat(ex.getMessage(), containsString("audit size limit"));
    }

    public void testRequestBodyWithinLimitSucceeds() throws Exception {
        // A body under the 10-byte limit (after rendering): {"a":"b"} is 9 bytes
        String body = "{\"a\":\"b\"}";
        Request request = new Request("PUT", "/test-index/_doc/1");
        request.setJsonEntity(body);
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(
                SecuritySettingsSource.ES_TEST_ROOT_USER,
                SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING
            )
        );
        request.setOptions(options);

        int statusCode = getRestClient().performRequest(request).getStatusLine().getStatusCode();
        assertThat(statusCode, is(201));
    }
}
