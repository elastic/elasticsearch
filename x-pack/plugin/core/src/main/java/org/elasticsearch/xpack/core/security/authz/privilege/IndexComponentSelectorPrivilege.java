/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.core.Predicates;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.set.Sets.newHashSet;

public record IndexComponentSelectorPrivilege(String name, Predicate<IndexComponentSelector> predicate) {
    public static final IndexComponentSelectorPrivilege ALL = new IndexComponentSelectorPrivilege("all", Predicates.always());
    public static final IndexComponentSelectorPrivilege DATA = new IndexComponentSelectorPrivilege(
        "data",
        IndexComponentSelector.DATA::equals
    );
    public static final IndexComponentSelectorPrivilege FAILURES = new IndexComponentSelectorPrivilege(
        "failures",
        IndexComponentSelector.FAILURES::equals
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

    public static Map<IndexComponentSelectorPrivilege, Set<String>> partitionBySelectorPrivilege(String... indexPrivileges) {
        return partitionBySelectorPrivilege(newHashSet(indexPrivileges));
    }

    public static Map<IndexComponentSelectorPrivilege, Set<String>> partitionBySelectorPrivilege(Set<String> indexPrivileges) {
        final Set<String> dataAccessPrivileges = new HashSet<>();
        final Set<String> failuresAccessPrivileges = new HashSet<>();

        for (String indexPrivilege : indexPrivileges) {
            final IndexComponentSelectorPrivilege selectorPrivilege = get(indexPrivilege);
            // If we ever hit `all`, the entire group can be treated as granting "all" access and we can return early
            if (selectorPrivilege == ALL) {
                return Map.of(ALL, indexPrivileges);
            }

            if (selectorPrivilege == DATA) {
                dataAccessPrivileges.add(indexPrivilege);
            } else if (selectorPrivilege == FAILURES) {
                failuresAccessPrivileges.add(indexPrivilege);
            } else {
                final var message = "index privilege [" + indexPrivilege + "] mapped to an unexpected selector [" + selectorPrivilege + "]";
                assert false : message;
                throw new IllegalStateException(message);
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
        // `null` means we got a raw action instead of a named privilege; all raw actions are treated as data access
        return indexPrivilege == null ? DATA : indexPrivilege.getSelectorPrivilege();
    }
}
