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
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.entitlement.qa.EntitlementsTestRule.PolicyBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractEntitlementsIT extends ESRestTestCase {

    static final PolicyBuilder ALLOWED_TEST_ENTITLEMENTS = (builder, tempDir) -> {
        builder.value("create_class_loader");
        builder.value("set_https_connection_properties");
        builder.value("inbound_network");
        builder.value("outbound_network");
        builder.value("load_native_libraries");
        builder.value("manage_threads");
        builder.value(
            Map.of(
                "write_system_properties",
                Map.of("properties", List.of("es.entitlements.checkSetSystemProperty", "es.entitlements.checkClearSystemProperty"))
            )
        );
        builder.value(
            Map.of(
                "files",
                List.of(
                    Map.of("path", tempDir.resolve("read_dir"), "mode", "read"),
                    Map.of("path", tempDir.resolve("read_write_dir"), "mode", "read_write"),
                    Map.of("path", tempDir.resolve("read_file"), "mode", "read"),
                    Map.of("path", tempDir.resolve("read_write_file"), "mode", "read_write"),
                    // Try to grant explicit access to forbidden files (and test this is not possible in any case)
                    Map.of("relative_path", "jvm.options.d", "relative_to", "config", "mode", "read_write"),
                    Map.of("relative_path", "jvm.options", "relative_to", "config", "mode", "read_write"),
                    Map.of("relative_path", "elasticsearch.yml", "relative_to", "config", "mode", "read_write")
                )
            )
        );
    };

    private final String actionName;
    private final boolean expectAllowed;

    AbstractEntitlementsIT(String actionName, boolean expectAllowed) {
        this.actionName = actionName;
        this.expectAllowed = expectAllowed;
    }

    private Response executeCheck() throws IOException {
        var request = new Request("GET", "/_entitlement_check");
        request.addParameter("action", actionName);
        return client().performRequest(request);
    }

    public void testAction() throws IOException {
        logger.info("Executing Entitlement test for [{}]", actionName);
        if (expectAllowed) {
            Response result = executeCheck();
            assertThat(result.getStatusLine().getStatusCode(), equalTo(200));
        } else {
            try {
                Response result = executeCheck();
                // If the call succeeded in a denied context, a default value strategy must be in play.
                // Verify the returned default matches the expected value.
                String expectedDefault = result.getHeader("expectedDefaultIfDenied");
                assertNotNull(
                    "Action [" + actionName + "] succeeded in denied context but has no expectedDefaultIfDenied",
                    expectedDefault
                );
                String actualValue = result.getHeader("resultValue");
                assertThat("Action [" + actionName + "] returned unexpected default value", actualValue, equalTo(expectedDefault));
            } catch (ResponseException exception) {
                assertThat(exception, statusCodeMatcher(403));
            }
        }
    }

    private static Matcher<ResponseException> statusCodeMatcher(int statusCode) {
        return new TypeSafeMatcher<>() {
            String expectedException = null;
            String mismatchDetail = null;

            @Override
            protected boolean matchesSafely(ResponseException item) {
                Response resp = item.getResponse();
                expectedException = resp.getHeader("expectedException");
                if (resp.getStatusLine().getStatusCode() != statusCode || expectedException == null) {
                    return false;
                }
                String notEntitledCause = resp.getHeader("notEntitledCause");
                if ("false".equals(notEntitledCause)) {
                    mismatchDetail = "expected NotEntitledException in cause chain but it was absent";
                    return false;
                }
                return true;
            }

            @Override
            public void describeTo(Description description) {
                description.appendValue(statusCode).appendText(" due to ").appendText(expectedException);
            }

            @Override
            protected void describeMismatchSafely(ResponseException item, Description description) {
                description.appendText("was ")
                    .appendValue(item.getResponse().getStatusLine().getStatusCode())
                    .appendText("\n")
                    .appendValue(item.getMessage());
                if (mismatchDetail != null) {
                    description.appendText("\n").appendText(mismatchDetail);
                }
            }
        };
    }
}
