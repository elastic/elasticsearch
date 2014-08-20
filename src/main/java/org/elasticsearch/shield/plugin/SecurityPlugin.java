/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.plugin;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.http.HttpServerModule;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.shield.SecurityModule;
import org.elasticsearch.shield.transport.netty.NettySecuredHttpServerTransport;
import org.elasticsearch.shield.transport.netty.NettySecuredTransport;
import org.elasticsearch.transport.TransportModule;

import java.util.Collection;

/**
 *
 */
public class SecurityPlugin extends AbstractPlugin {

    public static final String NAME = "shield";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public String description() {
        return "Elasticsearch Shield (security)";
    }

    @Override
    public Collection<Class<? extends Module>> modules() {
        return ImmutableList.<Class<? extends Module>>of(SecurityModule.class);
    }

}
