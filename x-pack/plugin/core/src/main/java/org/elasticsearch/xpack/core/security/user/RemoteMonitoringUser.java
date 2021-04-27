/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.security.support.MetadataUtils;

/**
 * Built in user for remote monitoring: collection as well as indexing.
 */
public class RemoteMonitoringUser extends User {

    public static final String NAME = UsernamesField.REMOTE_MONITORING_NAME;
    public static final String COLLECTION_ROLE_NAME = UsernamesField.REMOTE_MONITORING_COLLECTION_ROLE;
    public static final String INDEXING_ROLE_NAME = UsernamesField.REMOTE_MONITORING_INDEXING_ROLE;

    public RemoteMonitoringUser(boolean enabled) {
        super(NAME, new String[]{ COLLECTION_ROLE_NAME, INDEXING_ROLE_NAME }, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA, enabled);
    }
}
