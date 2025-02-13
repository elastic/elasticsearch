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

public enum IndexComponentSelectorPrivilege {
    ALL("all", (selector) -> true),
    DATA("data", IndexComponentSelector.DATA::equals),
    FAILURES("failures", IndexComponentSelector.FAILURES::equals);

    private final String name;
    private final Predicate<IndexComponentSelector> grants;

    IndexComponentSelectorPrivilege(String name, Predicate<IndexComponentSelector> grants) {
        this.name = name;
        this.grants = grants;
    }

    public String getName() {
        return name;
    }

    public boolean grants(IndexComponentSelector selector) {
        return grants.test(selector);
    }

    public boolean isTotal() {
        return this == ALL;
    }

    public static Set<IndexComponentSelectorPrivilege> get(Set<String> indexPrivileges) {
        return indexPrivileges.stream().map(IndexComponentSelectorPrivilege::get).collect(Collectors.toSet());
    }

    public static Map<IndexComponentSelectorPrivilege, Set<String>> split(Set<String> indexPrivileges) {
        final var data = new HashSet<String>();
        final var failures = new HashSet<String>();

        for (String indexPrivilege : indexPrivileges) {
            final var privilege = get(indexPrivilege);
            switch (privilege) {
                case DATA -> data.add(indexPrivilege);
                case FAILURES -> failures.add(indexPrivilege);
                // If we ever hit all, we can return early since we don't need to split
                case ALL -> {
                    return Map.of(ALL, indexPrivileges);
                }
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
        } else if (indexPrivilege == IndexPrivilege.READ_FAILURE_STORE || indexPrivilege == IndexPrivilege.MANAGE_FAILURE_STORE_INTERNAL) {
            return FAILURES;
        } else {
            return DATA;
        }
    }
}
