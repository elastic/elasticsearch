/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.common.Strings;

import java.util.regex.Pattern;

/**
 * Validates namespace and service name components for API-managed service accounts.
 * Built-in {@code elastic/*} principals continue to use the legacy
 * {@link org.elasticsearch.xpack.core.security.authc.service.ServiceAccount.ServiceAccountId} parsing rules.
 */
public final class ManagedServiceAccountIdValidator {

    public static final String BUILTIN_NAMESPACE = "elastic";
    /**
     * Document IDs use the {@code service_account-{principal}} form; keep components short enough to fit.
     */
    public static final int MAX_COMPONENT_LENGTH = 128;
    public static final int MAX_PRINCIPAL_LENGTH = MAX_COMPONENT_LENGTH * 2 + 1;
    private static final Pattern COMPONENT_PATTERN = Pattern.compile("^[a-zA-Z0-9][a-zA-Z0-9_-]*$");

    private ManagedServiceAccountIdValidator() {}

    public static String validateNamespace(String namespace) {
        if (Strings.isNullOrEmpty(namespace)) {
            return "service account namespace must not be empty";
        }
        if (BUILTIN_NAMESPACE.equals(namespace)) {
            return "the [" + BUILTIN_NAMESPACE + "] namespace is reserved for built-in service accounts";
        }
        return validateComponent(namespace, "namespace");
    }

    public static String validateServiceName(String serviceName) {
        if (Strings.isNullOrEmpty(serviceName)) {
            return "service account service name must not be empty";
        }
        return validateComponent(serviceName, "service name");
    }

    public static String validatePrincipal(String principal) {
        final int split = principal.indexOf('/');
        if (split == -1) {
            return "a service account ID must be in the form {namespace}/{service-name}, but was [" + principal + "]";
        }
        final String namespaceError = validateNamespace(principal.substring(0, split));
        if (namespaceError != null) {
            return namespaceError;
        }
        return validateServiceName(principal.substring(split + 1));
    }

    private static String validateComponent(String component, String label) {
        if (component.isBlank()) {
            return "service account " + label + " must not be blank";
        }
        if (component.equals(component.trim()) == false) {
            return "service account " + label + " must not have leading or trailing whitespace";
        }
        if (component.length() > MAX_COMPONENT_LENGTH) {
            return "service account " + label + " must be no more than " + MAX_COMPONENT_LENGTH + " characters";
        }
        if (component.indexOf('/') >= 0 || component.indexOf(':') >= 0) {
            return "service account " + label + " must not contain '/' or ':'";
        }
        if (COMPONENT_PATTERN.matcher(component).matches() == false) {
            return "service account "
                + label
                + " must start with an alphanumeric character and contain only letters, digits, hyphens, and underscores";
        }
        return null;
    }
}
