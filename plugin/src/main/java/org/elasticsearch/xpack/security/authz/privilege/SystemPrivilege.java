/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.privilege;

import org.elasticsearch.xpack.security.support.Automatons;

import java.util.Collections;
import java.util.function.Predicate;

public final class SystemPrivilege extends Privilege {

    public static SystemPrivilege INSTANCE = new SystemPrivilege();

    private static final Predicate<String> PREDICATE = Automatons.predicate(
            "internal:*",
            "indices:monitor/*", // added for monitoring
            "cluster:monitor/*",  // added for monitoring
            "cluster:admin/reroute", // added for DiskThresholdDecider.DiskListener
            "indices:admin/mapping/put" // needed for recovery and shrink api
    );

    private SystemPrivilege() {
        super(Collections.singleton("internal"));
    }

    @Override
    public Predicate<String> predicate() {
        return PREDICATE;
    }
}
