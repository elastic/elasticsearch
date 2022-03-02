/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.ldap.support;

import com.unboundid.ldap.sdk.SearchScope;

import org.elasticsearch.common.Strings;

import java.util.Locale;

public enum LdapSearchScope {

    BASE(SearchScope.BASE),
    ONE_LEVEL(SearchScope.ONE),
    SUB_TREE(SearchScope.SUB);

    private final SearchScope scope;

    LdapSearchScope(SearchScope scope) {
        this.scope = scope;
    }

    public SearchScope scope() {
        return scope;
    }

    public static LdapSearchScope resolve(String scope, LdapSearchScope defaultScope) {
        if (Strings.isNullOrEmpty(scope)) {
            return defaultScope;
        }
        return switch (scope.toLowerCase(Locale.ENGLISH)) {
            case "base", "object" -> BASE;
            case "one_level" -> ONE_LEVEL;
            case "sub_tree" -> SUB_TREE;
            default -> throw new IllegalArgumentException("unknown search scope [" + scope + "]");
        };
    }
}
