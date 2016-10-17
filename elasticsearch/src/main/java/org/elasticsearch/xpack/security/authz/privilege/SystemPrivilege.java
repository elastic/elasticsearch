/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.privilege;

import org.elasticsearch.xpack.security.support.AutomatonPredicate;

import java.util.function.Predicate;

import static org.elasticsearch.xpack.security.support.Automatons.patterns;

public class SystemPrivilege extends Privilege<SystemPrivilege> {

    public static SystemPrivilege INSTANCE = new SystemPrivilege();

    protected static final Predicate<String> PREDICATE = new AutomatonPredicate(patterns(
            "internal:*",
            "indices:monitor/*", // added for monitoring
            "cluster:monitor/*",  // added for monitoring
            "cluster:admin/reroute", // added for DiskThresholdDecider.DiskListener
            "indices:admin/mapping/put" // needed for recovery and shrink api
    ));

    SystemPrivilege() {
        super(new Name("internal"));
    }

    @Override
    public Predicate<String> predicate() {
        return PREDICATE;
    }

    @Override
    public boolean implies(SystemPrivilege other) {
        return true;
    }
}
