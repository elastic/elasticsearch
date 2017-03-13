/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.Version;
import org.elasticsearch.xpack.security.support.MetadataUtils;

public class BeatsSystemUser extends User {
    public static final String NAME = "beats_system";
    private static final String ROLE_NAME = "beats_system";
    public static final Version DEFINED_SINCE = Version.V_6_0_0_alpha1_UNRELEASED;
    public static final BuiltinUserInfo USER_INFO = new BuiltinUserInfo(NAME, ROLE_NAME, DEFINED_SINCE);

    public BeatsSystemUser(boolean enabled) {
        super(NAME, new String[]{ ROLE_NAME }, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA, enabled);
    }
}
