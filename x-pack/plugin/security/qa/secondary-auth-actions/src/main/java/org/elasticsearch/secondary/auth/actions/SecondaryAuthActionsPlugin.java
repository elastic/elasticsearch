/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.secondary.auth.actions;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.security.authc.support.SecondaryAuthActions;

import java.util.Set;

public class SecondaryAuthActionsPlugin extends Plugin implements SecondaryAuthActions {
    public Set<String> get() {
        return Set.of("cluster:admin/xpack/security/user/authenticate", "indices:admin/get");
    }
}
