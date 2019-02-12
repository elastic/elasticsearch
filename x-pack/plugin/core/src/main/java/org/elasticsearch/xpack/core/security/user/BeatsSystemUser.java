/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.Version;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;

/**
 * Built in user for beats internals. Currently used for Beats monitoring.
 */
public class BeatsSystemUser extends User {

    public static final String NAME = UsernamesField.BEATS_NAME;
    public static final String ROLE_NAME = UsernamesField.BEATS_ROLE;
    public static final Version DEFINED_SINCE = Version.V_6_3_0;

    public BeatsSystemUser(boolean enabled) {
        super(NAME, new String[]{ ROLE_NAME }, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA, enabled);
    }
}
