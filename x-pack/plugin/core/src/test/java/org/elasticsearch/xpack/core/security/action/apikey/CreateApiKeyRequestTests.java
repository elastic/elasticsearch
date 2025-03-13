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

import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CreateApiKeyRequestTests extends ESTestCase {

    public void testNameValidation() {
        final String name = randomAlphaOfLengthBetween(1, 256);
        CreateApiKeyRequest request = new CreateApiKeyRequest();
        request.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL, NONE));

        ActionRequestValidationException ve = request.validate();
        assertThat(ve.validationErrors().size(), is(1));
        assertThat(ve.validationErrors().get(0), containsString("api key name is required"));

        request.setName(name);
        ve = request.validate();
        assertNull(ve);

        request.setName(randomAlphaOfLength(257));
        ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), is(1));
        assertThat(ve.validationErrors().get(0), containsString("api key name may not be more than 256 characters long"));

        request.setName(" leading space");
        ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), is(1));
        assertThat(ve.validationErrors().get(0), containsString("api key name may not begin or end with whitespace"));

        request.setName("trailing space ");
        ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), is(1));
        assertThat(ve.validationErrors().get(0), containsString("api key name may not begin or end with whitespace"));

        request.setName(" leading and trailing space ");
        ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), is(1));
        assertThat(ve.validationErrors().get(0), containsString("api key name may not begin or end with whitespace"));

        request.setName("inner space");
        ve = request.validate();
        assertNull(ve);

        request.setName("_foo");
        ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), is(1));
        assertThat(ve.validationErrors().get(0), containsString("api key name may not begin with an underscore"));
    }

    public void testMetadataKeyValidation() {
        final String name = randomAlphaOfLengthBetween(1, 256);
        CreateApiKeyRequest request = new CreateApiKeyRequest();
        request.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL, NONE));
        request.setName(name);
        request.setMetadata(Map.of("_foo", "bar"));
        final ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), equalTo(1));
        assertThat(ve.validationErrors().get(0), containsString("API key metadata keys may not start with [_]"));
    }

    public void testRoleDescriptorValidation() {
        final String[] unknownWorkflows = randomArray(1, 2, String[]::new, () -> randomAlphaOfLengthBetween(4, 10));
        final CreateApiKeyRequest request1 = new CreateApiKeyRequest(
            randomAlphaOfLength(5),
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
                    new RoleDescriptor.Restriction(unknownWorkflows),
                    null
                )
            ),
            null
        );
        request1.setRefreshPolicy(randomFrom(IMMEDIATE, WAIT_UNTIL, NONE));
        final ActionRequestValidationException ve1 = request1.validate();
        assertNotNull(ve1);
        assertThat(ve1.validationErrors().get(0), containsString("unknown cluster privilege"));
        assertThat(ve1.validationErrors().get(1), containsString("unknown index privilege"));
        assertThat(ve1.validationErrors().get(2), containsStringIgnoringCase("application name"));
        assertThat(ve1.validationErrors().get(3), containsStringIgnoringCase("Application privilege names"));
        assertThat(ve1.validationErrors().get(4), containsStringIgnoringCase("role descriptor metadata keys may not start with "));
        for (int i = 0; i < unknownWorkflows.length; i++) {
            assertThat(ve1.validationErrors().get(5 + i), containsStringIgnoringCase("unknown workflow [" + unknownWorkflows[i] + "]"));
        }
    }
}
