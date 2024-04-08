/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;

public class BulkUpdateApiKeyRequestTests extends ESTestCase {
    public void testNullValuesValidForNonIds() {
        final var request = BulkUpdateApiKeyRequest.usingApiKeyIds("id");
        assertNull(request.validate());
    }

    public void testEmptyIdsNotValid() {
        final var request = new BulkUpdateApiKeyRequest(List.of(), null, null, null);
        final ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), equalTo(1));
        assertThat(ve.validationErrors().get(0), containsString("Field [ids] cannot be empty"));
    }

    public void testMetadataKeyValidation() {
        final var reservedKey = "_" + randomAlphaOfLengthBetween(0, 10);
        final var metadataValue = randomAlphaOfLengthBetween(1, 10);
        final TimeValue expiration = ApiKeyTests.randomFutureExpirationTime();
        final var request = new BulkUpdateApiKeyRequest(
            randomList(1, 5, () -> randomAlphaOfLength(10)),
            null,
            Map.of(reservedKey, metadataValue),
            expiration
        );
        final ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().size(), equalTo(1));
        assertThat(ve.validationErrors().get(0), containsString("API key metadata keys may not start with [_]"));
    }

    public void testRoleDescriptorValidation() {
        final String[] unknownWorkflows = randomArray(1, 2, String[]::new, () -> randomAlphaOfLengthBetween(4, 10));
        final var request = new BulkUpdateApiKeyRequest(
            randomList(1, 5, () -> randomAlphaOfLength(10)),
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
                    new RoleDescriptor.Restriction(unknownWorkflows)
                )
            ),
            null,
            null
        );
        final ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertThat(ve.validationErrors().get(0), containsString("unknown cluster privilege"));
        assertThat(ve.validationErrors().get(1), containsString("unknown index privilege"));
        assertThat(ve.validationErrors().get(2), containsStringIgnoringCase("application name"));
        assertThat(ve.validationErrors().get(3), containsStringIgnoringCase("Application privilege names"));
        assertThat(ve.validationErrors().get(4), containsStringIgnoringCase("role descriptor metadata keys may not start with "));
        for (int i = 0; i < unknownWorkflows.length; i++) {
            assertThat(ve.validationErrors().get(5 + i), containsStringIgnoringCase("unknown workflow [" + unknownWorkflows[i] + "]"));
        }
    }
}
