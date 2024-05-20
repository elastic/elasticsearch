/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

/**
 * The reserved {@code elastic} superuser. Has full permission/access to the cluster/indices and can
 * run as any other user.
 */
public class ElasticUser extends ReservedUser {

    public static final String NAME = UsernamesField.ELASTIC_NAME;
    // used for testing in a different package
    public static final String ROLE_NAME = UsernamesField.ELASTIC_ROLE;

    public ElasticUser(boolean enabled) {
        super(NAME, ROLE_NAME, enabled);
    }
}
