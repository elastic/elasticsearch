/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.user;

import org.elasticsearch.xpack.core.security.support.MetadataUtils;

import java.util.Optional;

public abstract class ReservedUser extends User {
    protected ReservedUser(String username, String role, boolean enabled) {
        this(username, new String[] { role }, enabled, Optional.empty());
    }

    protected ReservedUser(String username, String[] roles, boolean enabled, Optional<String> deprecation) {
        super(
            username,
            roles,
            null,
            null,
            deprecation.map(MetadataUtils::getDeprecatedReservedMetadata).orElse(MetadataUtils.DEFAULT_RESERVED_METADATA),
            enabled
        );
    }
}
