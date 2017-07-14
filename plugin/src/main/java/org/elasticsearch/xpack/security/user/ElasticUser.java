/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.user;

import org.elasticsearch.xpack.security.support.MetadataUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * The reserved {@code elastic} superuser. Has full permission/access to the cluster/indices and can
 * run as any other user.
 */
public class ElasticUser extends User {

    public static final String NAME = "elastic";
    private static final String ROLE_NAME = "superuser";

    public ElasticUser(boolean enabled) {
        super(NAME, new String[] { ROLE_NAME }, null, null, MetadataUtils.DEFAULT_RESERVED_METADATA, enabled);
    }
}
