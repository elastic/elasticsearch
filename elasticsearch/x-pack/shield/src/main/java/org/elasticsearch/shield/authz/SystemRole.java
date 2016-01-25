/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.shield.authz.privilege.SystemPrivilege;

import java.util.function.Predicate;

/**
 *
 */
public class SystemRole {

    public static final SystemRole INSTANCE = new SystemRole();

    public static final String NAME = "__es_system_role";

    private static final Predicate<String> PREDICATE = SystemPrivilege.INSTANCE.predicate();

    private SystemRole() {
    }

    public boolean check(String action) {
        return PREDICATE.test(action);
    }
}
