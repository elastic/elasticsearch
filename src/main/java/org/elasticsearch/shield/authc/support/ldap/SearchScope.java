/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support.ldap;

import org.elasticsearch.shield.authc.ldap.LdapException;

import javax.naming.directory.SearchControls;

/**
 *
 */
public enum SearchScope {

    BASE(SearchControls.OBJECT_SCOPE),
    ONE_LEVEL(SearchControls.ONELEVEL_SCOPE),
    SUB_TREE(SearchControls.SUBTREE_SCOPE);

    private final int scope;

    SearchScope(int scope) {
        this.scope = scope;
    }

    public int scope() {
        return scope;
    }

    public static SearchScope resolve(String scope, SearchScope defaultScope) {
        if (scope == null) {
            return defaultScope;
        }
        switch (scope.toLowerCase()) {
            case "base":
            case "object": return BASE;
            case "one_level" : return ONE_LEVEL;
            case "sub_tree" : return SUB_TREE;
            default:
                throw new LdapException("Unknown search scope [" + scope + "]");
        }
    }
}
