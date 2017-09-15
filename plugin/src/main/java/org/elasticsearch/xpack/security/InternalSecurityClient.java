/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.user.XPackSecurityUser;

/**
 * A special filter client for internal usage by security to modify the security index.
 *
 * The {@link XPackSecurityUser} user is added to the execution context before each action is executed.
 */
public class InternalSecurityClient extends InternalClient {

    public InternalSecurityClient(Settings settings, ThreadPool threadPool, Client in) {
        super(settings, threadPool, in, XPackSecurityUser.INSTANCE);
    }
}
