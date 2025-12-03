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
    }

    private static void validateAndAssertSelectorNotAllowed(RoleDescriptor roleDescriptor, String indexName) {
        var validationException = RoleDescriptorRequestValidator.validate(roleDescriptor);
        assertThat("expected validation exception for " + indexName, validationException, notNullValue());
        assertThat(
            validationException.validationErrors(),
            containsInAnyOrder("selectors [::] are not allowed in the index name expression [" + indexName + "]")
        );
    }

    public void testUppercaseIndexExpressionValidation() {
        String[] invalidIndexNames = { "MY-INDEX", "My-Index", "my-INDEX-*", "MY-*", "*-SUFFIX", "pre?X-UPPER", "logs-2024.01.?-UPPER" };
        for (String indexName : invalidIndexNames) {
            validateAndAssertLowercaseRequired(roleWithIndexPrivileges(indexName), indexName);
            validateAndAssertLowercaseRequired(roleWithRemoteIndexPrivileges(indexName), indexName);
        }
    }

    public void testLuceneRegexValidation() {
        String[] invalidRegexPatterns = {
            "/",           // incomplete regex
            "/unclosed",   // missing closing /
            "/[invalid/",  // malformed regex syntax
            "/(/",         // unbalanced parentheses
            "/[a-/" };     // incomplete character class
        for (String pattern : invalidRegexPatterns) {
            validateAndAssertInvalidRegex(roleWithIndexPrivileges(pattern), pattern);
            validateAndAssertInvalidRegex(roleWithRemoteIndexPrivileges(pattern), pattern);
        }

        String[] validRegexPatterns = {
            "/logs-.*/",
            "/[a-z]+/",
            "/[abc]{2,5}/",
            "/index-[0-9]{4}/",
            "/.*/",
            "/*/",         // valid Lucene regex (matches any string)
            "/logs-[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}/" };
        for (String pattern : validRegexPatterns) {
            validateAndAssertNoException(roleWithIndexPrivileges(pattern), pattern);
            validateAndAssertNoException(roleWithRemoteIndexPrivileges(pattern), pattern);
        }
    }

    public void testInvalidCharactersValidation() {
        String[] invalidIndexNames = {
            "my index",    // space
            "my,index",    // comma
            "my\"index",   // quote
            "my<index",    // less than
            "my>index",    // greater than
            "my|index",    // pipe
            "my/index",    // forward slash (not a regex)
            "#hashtag",    // hash
            "_private",    // leading underscore
            "-dashstart",  // leading dash
            "+plusstart",  // leading plus
            ".",           // dot only
            ".." };        // double dot
        for (String indexName : invalidIndexNames) {
            validateAndAssertInvalidExpression(roleWithIndexPrivileges(indexName), indexName);
            validateAndAssertInvalidExpression(roleWithRemoteIndexPrivileges(indexName), indexName);
        }
    }

    public void testValidWildcardPatterns() {
        String[] validIndexNames = {
            "*",                    // match all
            "logs-*",               // prefix wildcard
            "*-logs",               // suffix wildcard
            "*-logs-*",             // infix
            "logs-2024.*",          // dot prefix
            "logs-?",               // single char wildcard
            "logs-????-*",          // multiple wildcards
            ".hidden-*",            // hidden index pattern
            "my-index",             // exact match
            "my_index",             // underscore
            "my.index.name",        // dots
            "123-numeric",          // starts with number
            "logs-2024.01.01" };    // date pattern
        for (String indexName : validIndexNames) {
            validateAndAssertNoException(roleWithIndexPrivileges(indexName), indexName);
            validateAndAssertNoException(roleWithRemoteIndexPrivileges(indexName), indexName);
        }
    }

    public void testEscapeCharacterHandling() {
        // Escaped backslash becomes literal backslash - which is invalid in index names
        // So \\-data stripped becomes \-data which contains invalid backslash
        String[] invalidEscapedPatterns = {
            "logs-\\*-data",        // escaped asterisk becomes literal * (invalid char)
            "logs-\\?-data",        // escaped question mark becomes literal ? (invalid char)
            "logs-\\\\-data",       // escaped backslash becomes literal \ (invalid char)
            "logs-\\ -data",        // escaped space is still invalid
            "logs-\\,-data" };      // escaped comma is still invalid
        for (String pattern : invalidEscapedPatterns) {
            validateAndAssertInvalidExpression(roleWithIndexPrivileges(pattern), pattern);
            validateAndAssertInvalidExpression(roleWithRemoteIndexPrivileges(pattern), pattern);
        }

        // Escaping letters is valid (just becomes the letter)
        String[] validEscapedPatterns = {
            "logs-\\a-data",        // escaped 'a' becomes literal 'a'
            "logs-\\x-data" };      // escaped 'x' becomes literal 'x'
        for (String pattern : validEscapedPatterns) {
            validateAndAssertNoException(roleWithIndexPrivileges(pattern), pattern);
            validateAndAssertNoException(roleWithRemoteIndexPrivileges(pattern), pattern);
        }
    }

    private static void validateAndAssertInvalidExpression(RoleDescriptor roleDescriptor, String indexName) {
        var validationException = RoleDescriptorRequestValidator.validate(roleDescriptor);
        assertThat("expected validation exception for " + indexName, validationException, notNullValue());
        assertThat(validationException.validationErrors(), containsInAnyOrder("invalid index name expression [" + indexName + "]"));
    }

    private static void validateAndAssertLowercaseRequired(RoleDescriptor roleDescriptor, String indexName) {
        var validationException = RoleDescriptorRequestValidator.validate(roleDescriptor);
        assertThat("expected validation exception for " + indexName, validationException, notNullValue());
        assertThat(validationException.validationErrors(), containsInAnyOrder("index name must be lowercase [" + indexName + "]"));
    }

    private static void validateAndAssertInvalidRegex(RoleDescriptor roleDescriptor, String pattern) {
        var validationException = RoleDescriptorRequestValidator.validate(roleDescriptor);
        assertThat("expected validation exception for " + pattern, validationException, notNullValue());
        assertThat(validationException.validationErrors(), containsInAnyOrder("invalid regular expression pattern [" + pattern + "]"));
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
