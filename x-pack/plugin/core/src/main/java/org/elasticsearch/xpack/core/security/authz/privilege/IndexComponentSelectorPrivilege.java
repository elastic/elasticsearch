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

public record IndexComponentSelectorPrivilege(Set<String> names, Predicate<IndexComponentSelector> predicate)
    implements
        Predicate<IndexComponentSelector> {
    IndexComponentSelectorPrivilege(String name, Predicate<IndexComponentSelector> predicate) {
        this(Set.of(name), predicate);
    }

    public static final IndexComponentSelectorPrivilege ALL = new IndexComponentSelectorPrivilege("all", Predicates.always());
    public static final IndexComponentSelectorPrivilege DATA = new IndexComponentSelectorPrivilege(
        "data",
        IndexComponentSelector.DATA::equals
    );
    public static final IndexComponentSelectorPrivilege FAILURES = new IndexComponentSelectorPrivilege(
        "failures",
        IndexComponentSelector.FAILURES::equals
    );

    @Override
    public boolean test(IndexComponentSelector selector) {
        return predicate.test(selector);
    }
}
