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
    private static final String SETUP_MODE = "_setup_mode";

    public ElasticUser(boolean enabled) {
        this(enabled, false);
    }

    public ElasticUser(boolean enabled, boolean setupMode) {
        super(NAME, new String[] { ROLE_NAME }, null, null, metadata(setupMode), enabled);
    }

    public static boolean isElasticUserInSetupMode(User user) {
        return NAME.equals(user.principal()) && Boolean.TRUE.equals(user.metadata().get(SETUP_MODE));
    }

    private static Map<String, Object> metadata(boolean setupMode) {
        if (setupMode == false) {
            return MetadataUtils.DEFAULT_RESERVED_METADATA;
        } else {
            HashMap<String, Object> metadata = new HashMap<>(MetadataUtils.DEFAULT_RESERVED_METADATA);
            metadata.put(SETUP_MODE, true);
            return metadata;
        }
    }
}
