/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges.ManageServiceAccountsPrivilege;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The user-managed service accounts a caller may read only because they hold {@link ManageServiceAccountsPrivilege}
 * grants, as opposed to a privilege that reads every account. Authorization attaches it to a read request that would
 * otherwise be denied, and the action then reports only the accounts it admits.
 * <p>
 * Each group is the union of the account name patterns from the grants of one role, so any pattern in a group admits
 * an account. A caller whose access is the intersection of several roles, such as an API key and the user who created
 * it, has one group per role that restricts it, and an account is admitted only when every group admits it.
 */
public final class AuthorizedServiceAccounts implements Predicate<String> {

    private final List<Set<String>> patternGroups;
    private final List<StringMatcher> matchers;

    public AuthorizedServiceAccounts(List<Set<String>> patternGroups) {
        if (patternGroups.isEmpty() || patternGroups.stream().anyMatch(Set::isEmpty)) {
            throw new IllegalArgumentException("authorized service accounts need at least one non-empty group of patterns");
        }
        this.patternGroups = patternGroups.stream().map(Set::copyOf).toList();
        this.matchers = this.patternGroups.stream().map(StringMatcher::of).toList();
    }

    public List<Set<String>> patternGroups() {
        return patternGroups;
    }

    /**
     * Whether the account with the given {@code namespace/service-name} principal may be read.
     */
    @Override
    public boolean test(String principal) {
        return matchers.stream().allMatch(matcher -> matcher.test(principal));
    }

    @Override
    public String toString() {
        return "AuthorizedServiceAccounts{" + patternGroups + "}";
    }
}
