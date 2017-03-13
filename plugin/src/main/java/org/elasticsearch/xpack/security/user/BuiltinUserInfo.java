/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.Version;

/**
 * BuiltinUserInfo provides common user meta data for newly introduced pre defined System Users.
 */
public class BuiltinUserInfo {
    private final String name;
    private final String role;
    private final Version definedSince;

    public BuiltinUserInfo(String name, String role, Version definedSince) {
        this.name = name;
        this.role = role;
        this.definedSince = definedSince;
    }

    /** Get the builtin users name. */
    public String getName() {
        return name;
    }

    /** Get the builtin users default role name. */
    public String getRole() {
        return role;
    }

    /** Get version the builtin user was introduced with. */
    public Version getDefinedSince() {
        return definedSince;
    }
}
