/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.support.IndexComponentSelector;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public record IndexComponentSelectorPrivilege(String name, Predicate<IndexComponentSelector> predicate) {
    public static final IndexComponentSelectorPrivilege ALL = new IndexComponentSelectorPrivilege("all", (selector) -> true);
    public static final IndexComponentSelectorPrivilege DATA = new IndexComponentSelectorPrivilege(
        "data",
        IndexComponentSelector.DATA::equals
    );
    public static final IndexComponentSelectorPrivilege FAILURES = new IndexComponentSelectorPrivilege(
        "failures",
        IndexComponentSelector.FAILURES::equals
    );

    private static final Set<IndexPrivilege> FAILURE_STORE_PRIVILEGE_NAMES = Set.of(
        IndexPrivilege.READ_FAILURE_STORE,
        IndexPrivilege.MANAGE_FAILURE_STORE_INTERNAL
    );

    public boolean grants(IndexComponentSelector selector) {
        return predicate.test(selector);
    }

    public boolean isTotal() {
        return this == ALL;
    }

    public static Set<IndexComponentSelectorPrivilege> get(Set<String> indexPrivileges) {
        return indexPrivileges.stream().map(IndexComponentSelectorPrivilege::get).collect(Collectors.toSet());
    }

    public static Map<IndexComponentSelectorPrivilege, Set<String>> groupBySelectors(String... indexPrivileges) {
        return groupBySelectors(Set.of(indexPrivileges));
    }

    public static Map<IndexComponentSelectorPrivilege, Set<String>> groupBySelectors(Set<String> indexPrivileges) {
        final Set<String> dataAccessPrivileges = new HashSet<>();
        final Set<String> failuresAccessPrivileges = new HashSet<>();

        for (String indexPrivilege : indexPrivileges) {
            final IndexComponentSelectorPrivilege selectorPrivilege = get(indexPrivilege);
            // If we ever hit `all`, the entire group can be treated as granting "all" access and we can return early
            if (selectorPrivilege.equals(ALL)) {
                return Map.of(ALL, indexPrivileges);
            }

            if (selectorPrivilege.equals(DATA)) {
                dataAccessPrivileges.add(indexPrivilege);
            } else if (selectorPrivilege.equals(FAILURES)) {
                failuresAccessPrivileges.add(indexPrivilege);
            } else {
                assert false : "index privilege [" + indexPrivilege + "] mapped to an unexpected selector [" + selectorPrivilege + "]";
                throw new IllegalStateException(
                    "index privilege [" + indexPrivilege + "] mapped to an unexpected selector [" + selectorPrivilege + "]"
                );
            }
        }

        if (dataAccessPrivileges.isEmpty()) {
            return Map.of(FAILURES, failuresAccessPrivileges);
        } else if (failuresAccessPrivileges.isEmpty()) {
            return Map.of(DATA, dataAccessPrivileges);
        } else {
            return Map.of(DATA, dataAccessPrivileges, FAILURES, failuresAccessPrivileges);
        }
    }

    private static IndexComponentSelectorPrivilege get(String indexPrivilegeName) {
        final IndexPrivilege indexPrivilege = IndexPrivilege.getNamedOrNull(indexPrivilegeName);
        if (indexPrivilege == null) {
            return DATA;
        } else if (indexPrivilege == IndexPrivilege.ALL) {
            return ALL;
        } else if (FAILURE_STORE_PRIVILEGE_NAMES.contains(indexPrivilege)) {
            return FAILURES;
        } else {
            return DATA;
        }
    }
}
