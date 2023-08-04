/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import java.util.Optional;

/**
 * Built in user for the kibana server
 * @deprecated use KibanaSystemUser
 */
@Deprecated
public class KibanaUser extends ReservedUser {

    public static final String NAME = UsernamesField.DEPRECATED_KIBANA_NAME;
    public static final String ROLE_NAME = UsernamesField.KIBANA_ROLE;

    public KibanaUser(boolean enabled) {
        super(NAME, new String[] { ROLE_NAME }, enabled, Optional.of("Please use the [kibana_system] user instead."));
    }
}
