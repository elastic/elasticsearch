/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RoleDescriptorRequestValidatorTests extends ESTestCase {

    public void testSelectorsValidation() {
        String[] invalidIndexNames = {
            "index::failures",
            ".fs-*::failures",
            ".ds-*::data",
            "*::failures",
            "*::",
            "?::?",
            "test?-*::data",
            "test-*::*", // actual selector is not relevant and not validated
            "test::irrelevant",
            "::test",
            "test::",
            "::",
            ":: ",
            " ::",
            ":::",
            "::::",
            randomAlphaOfLengthBetween(5, 10) + "\u003a\u003afailures",
            randomAlphaOfLengthBetween(5, 10) + "\072\072failures" };
        for (String indexName : invalidIndexNames) {
            validateAndAssertSelectorNotAllowed(roleWithIndexPrivileges(indexName), indexName);
            validateAndAssertSelectorNotAllowed(roleWithRemoteIndexPrivileges(indexName), indexName);
        }

        // these are not necessarily valid index names, but they should not trigger the selector validation
        String[] validIndexNames = {
            "index:failures", // single colon is allowed
            ":failures",
            "no double colon",
            ":",
            ": :",
            "",
            " ",
            ":\n:",
            null,
            "a:",
            ":b:",
            "*",
            "c?-*",
            "d-*e",
            "f:g:h",
            "/[a-b]*test:[a-b]*:failures/", // while this regex can match test::failures, it is not rejected - doing so would be too complex
            randomIntBetween(-10, 10) + "",
            randomAlphaOfLengthBetween(1, 10),
            randomAlphanumericOfLength(10) };
        for (String indexName : validIndexNames) {
            validateAndAssertNoException(roleWithIndexPrivileges(indexName), indexName);
            validateAndAssertNoException(roleWithRemoteIndexPrivileges(indexName), indexName);
        }
    }

    private static void validateAndAssertSelectorNotAllowed(RoleDescriptor roleDescriptor, String indexName) {
        var validationException = RoleDescriptorRequestValidator.validate(roleDescriptor);
        assertThat("expected validation exception for " + indexName, validationException, notNullValue());
        assertThat(
            validationException.validationErrors(),
            containsInAnyOrder("selectors [::] are not allowed in the index name expression [" + indexName + "]")
        );
    }

    private static void validateAndAssertNoException(RoleDescriptor roleDescriptor, String indexName) {
        var validationException = RoleDescriptorRequestValidator.validate(roleDescriptor);
        assertThat("expected no validation exception for " + indexName, validationException, nullValue());
    }

    private static RoleDescriptor roleWithIndexPrivileges(String... indices) {
        return new RoleDescriptor(
            "test-role",
            null,
            new IndicesPrivileges[] {
                IndicesPrivileges.builder()
                    .allowRestrictedIndices(randomBoolean())
                    .indices(indices)
                    .privileges(randomSubsetOf(randomIntBetween(1, IndexPrivilege.names().size()), IndexPrivilege.names()))
                    .build() },
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    private static RoleDescriptor roleWithRemoteIndexPrivileges(String... indices) {
        Set<String> privileges = IndexPrivilege.names()
            .stream()
            .filter(p -> false == (p.equals("read_failure_store") || p.equals("manage_failure_store")))
            .collect(Collectors.toSet());
        return new RoleDescriptor(
            "remote-test-role",
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new RoleDescriptor.RemoteIndicesPrivileges[] {
                new RoleDescriptor.RemoteIndicesPrivileges(
                    IndicesPrivileges.builder()
                        .allowRestrictedIndices(randomBoolean())
                        .indices(indices)
                        .privileges(randomSubsetOf(randomIntBetween(1, privileges.size()), privileges))
                        .build(),
                    "my-remote-cluster"
                ) },
            null,
            null,
            null
        );
    }
}
