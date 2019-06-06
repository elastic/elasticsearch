/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
        switch (scope.toLowerCase(Locale.ENGLISH)) {
            case "base":
            case "object": return BASE;
            case "one_level" : return ONE_LEVEL;
            case "sub_tree" : return SUB_TREE;
            default:
                throw new IllegalArgumentException("unknown search scope [" + scope + "]");
        }
    }
}
