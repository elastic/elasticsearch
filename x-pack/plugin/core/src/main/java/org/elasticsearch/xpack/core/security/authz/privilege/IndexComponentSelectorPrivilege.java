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

    public boolean test(IndexComponentSelector selector) {
        return predicate.test(selector);
    }

    public boolean isTotal() {
        return this == ALL;
    }

    public static Set<IndexComponentSelectorPrivilege> get(Set<String> indexPrivileges) {
        return indexPrivileges.stream().map(IndexComponentSelectorPrivilege::get).collect(Collectors.toSet());
    }

    public static Map<IndexComponentSelectorPrivilege, Set<String>> splitBySelectors(String... indexPrivileges) {
        return splitBySelectors(Set.of(indexPrivileges));
    }

    public static Map<IndexComponentSelectorPrivilege, Set<String>> splitBySelectors(Set<String> indexPrivileges) {
        final Set<String> data = new HashSet<>();
        final Set<String> failures = new HashSet<>();

        for (String indexPrivilege : indexPrivileges) {
            final IndexComponentSelectorPrivilege privilege = get(indexPrivilege);
            // If we ever hit all, we can return early since we don't need to split
            if (privilege.equals(ALL)) {
                return Map.of(ALL, indexPrivileges);
            }

            if (privilege.equals(DATA)) {
                data.add(indexPrivilege);
            } else if (privilege.equals(FAILURES)) {
                failures.add(indexPrivilege);
            } else {
                throw new IllegalArgumentException("Unknown index privilege: " + indexPrivilege);
            }

        }

        if (data.isEmpty()) {
            return Map.of(FAILURES, failures);
        } else if (failures.isEmpty()) {
            return Map.of(DATA, data);
        } else {
            return Map.of(DATA, data, FAILURES, failures);
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
