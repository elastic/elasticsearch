/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.security.support.MetadataUtils;

/**
 * Built in user for APM server internals. Currently used for APM server monitoring.
 */
public class APMSystemUser extends User {

    public static final String NAME = UsernamesField.APM_NAME;
    public static final String ROLE_NAME = UsernamesField.APM_ROLE;

    public APMSystemUser(boolean enabled) {
        super(NAME, new String[]{ ROLE_NAME }, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA, enabled);
    }
}
