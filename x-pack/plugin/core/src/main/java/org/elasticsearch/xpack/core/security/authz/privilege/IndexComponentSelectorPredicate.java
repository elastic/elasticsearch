/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.privilege;

import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.core.Predicates;

import java.util.Set;
import java.util.function.Predicate;

/**
 * A predicate to capture role access by {@link IndexComponentSelector}.
 * This is assigned to each {@link org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission.Group} during role building.
 * See also {@link org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege#resolveBySelectorAccess(Set)}.
 */
public record IndexComponentSelectorPredicate(Set<String> names, Predicate<IndexComponentSelector> predicate)
    implements
        Predicate<IndexComponentSelector> {
    IndexComponentSelectorPredicate(String name, Predicate<IndexComponentSelector> predicate) {
        this(Set.of(name), predicate);
    }

    public static final IndexComponentSelectorPredicate ALL = new IndexComponentSelectorPredicate("all", Predicates.always());
    public static final IndexComponentSelectorPredicate DATA = new IndexComponentSelectorPredicate(
        "data",
        IndexComponentSelector.DATA::equals
    );
    public static final IndexComponentSelectorPredicate FAILURES = new IndexComponentSelectorPredicate(
        "failures",
        IndexComponentSelector.FAILURES::equals
    );
    public static final IndexComponentSelectorPredicate DATA_AND_FAILURES = new IndexComponentSelectorPredicate(
        Set.of("data", "failures"),
        DATA.predicate.or(FAILURES.predicate)
    );

    @Override
    public boolean test(IndexComponentSelector selector) {
        return predicate.test(selector);
    }
}
