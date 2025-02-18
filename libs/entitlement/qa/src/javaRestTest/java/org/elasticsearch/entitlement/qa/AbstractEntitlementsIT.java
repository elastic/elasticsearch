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
import org.elasticsearch.entitlement.qa.EntitlementsTestRule.PolicyBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
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
                    Map.of("path", tempDir.resolve("read_write_file"), "mode", "read_write")
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
            var exception = expectThrows(IOException.class, this::executeCheck);
            assertThat(exception.getMessage(), containsString("not_entitled_exception"));
        }
    }
}
