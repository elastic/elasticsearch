/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;

public class UpdateApiKeyRequestTests extends ESTestCase {

    public void testNullValuesValidForNonIds() {
        final var request = new UpdateApiKeyRequest("id", null, null, null);
        assertNull(request.validate());
    }

    public void testMetadataKeyValidation() {
        final var reservedKey = "_" + randomAlphaOfLengthBetween(0, 10);
        final var metadataValue = randomAlphaOfLengthBetween(1, 10);

        UpdateApiKeyRequest request = new UpdateApiKeyRequest(randomAlphaOfLength(10), null, Map.of(reservedKey, metadataValue), null);
        final ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), equalTo(1));
        assertThat(ve.validationErrors().get(0), containsString("API key metadata keys may not start with [_]"));
    }

    public void testRoleDescriptorValidation() {
        final List<String> unknownWorkflows = randomList(1, 2, () -> randomAlphaOfLengthBetween(4, 10));
        final List<String> workflows = new ArrayList<>(unknownWorkflows.size() + 1);
        workflows.addAll(unknownWorkflows);
        workflows.add(WorkflowResolver.SEARCH_APPLICATION_QUERY_WORKFLOW.name());
        final var request1 = new UpdateApiKeyRequest(
            randomAlphaOfLength(10),
            List.of(
                new RoleDescriptor(
                    randomAlphaOfLength(5),
                    new String[] { "manage_index_template" },
                    new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder().indices("*").privileges("rad").build() },
                    new RoleDescriptor.ApplicationResourcePrivileges[] {
                        RoleDescriptor.ApplicationResourcePrivileges.builder()
                            .application(randomFrom("app*tab", "app 1"))
                            .privileges(randomFrom(" ", "\n"))
                            .resources("resource")
                            .build() },
                    null,
                    null,
                    Map.of("_key", "value"),
                    null,
                    null,
                    null,
                    new RoleDescriptor.Restriction(workflows.toArray(String[]::new)),
                    null
                )
            ),
            null,
            null
        );
        final ActionRequestValidationException ve1 = request1.validate();
        assertNotNull(ve1);
        assertThat(ve1.validationErrors().get(0), containsString("unknown cluster privilege"));
        assertThat(ve1.validationErrors().get(1), containsString("unknown index privilege"));
        assertThat(ve1.validationErrors().get(2), containsStringIgnoringCase("application name"));
        assertThat(ve1.validationErrors().get(3), containsStringIgnoringCase("Application privilege names"));
        assertThat(ve1.validationErrors().get(4), containsStringIgnoringCase("role descriptor metadata keys may not start with "));
        for (int i = 0; i < unknownWorkflows.size(); i++) {
            assertThat(ve1.validationErrors().get(5 + i), containsStringIgnoringCase("unknown workflow [" + unknownWorkflows.get(i) + "]"));
        }
    }
}
