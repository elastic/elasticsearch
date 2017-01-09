/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.xpack.security.support.MetadataUtils;

/**
 * Built in user for the kibana server
 */
public class KibanaUser extends User {

    public static final String NAME = "kibana";
    public static final String ROLE_NAME = "kibana_system";

    public KibanaUser(boolean enabled) {
        super(NAME, new String[]{ ROLE_NAME }, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA, enabled);
    }
}
