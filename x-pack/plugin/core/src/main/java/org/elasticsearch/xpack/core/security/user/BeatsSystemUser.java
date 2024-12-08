/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.user;

/**
 * Built in user for beats internals. Currently used for Beats monitoring.
 */
public class BeatsSystemUser extends ReservedUser {

    public static final String NAME = UsernamesField.BEATS_NAME;

    public BeatsSystemUser(boolean enabled) {
        super(NAME, UsernamesField.BEATS_ROLE, enabled);
    }
}
