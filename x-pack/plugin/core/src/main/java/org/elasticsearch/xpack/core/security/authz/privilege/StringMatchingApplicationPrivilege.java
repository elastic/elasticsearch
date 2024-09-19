/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.Set;

public class StringMatchingApplicationPrivilege extends ApplicationPrivilege {
    private final StringMatcher stringMatcher;

    StringMatchingApplicationPrivilege(String application, Set<String> name, String... patterns) {
        super(application, name, patterns);
        this.stringMatcher = StringMatcher.of(patterns);
    }

    @Override
    public boolean patternsTotal() {
        return stringMatcher.isTotal();
    }

    @Override
    public boolean patternsEmpty() {
        return stringMatcher.isEmpty();
    }

    @Override
    public boolean supersetOfPatterns(ApplicationPrivilege other) {
        return supersetOfPatterns(other.getPatterns());
    }

    @Override
    public boolean supersetOfPatterns(String... patterns) {
        for (var pattern : patterns) {
            if (false == stringMatcher.test(pattern)) {
                return false;
            }
        }
        return true;
    }
}
