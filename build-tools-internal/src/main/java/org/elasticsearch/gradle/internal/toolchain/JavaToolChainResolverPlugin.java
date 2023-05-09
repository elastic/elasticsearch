/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.toolchain;

import org.gradle.api.Plugin;
import org.gradle.api.initialization.Settings;
import org.gradle.jvm.toolchain.JavaToolchainResolverRegistry;

import javax.inject.Inject;

public abstract class JavaToolChainResolverPlugin implements Plugin<Settings> {
    @Inject
    protected abstract JavaToolchainResolverRegistry getToolchainResolverRegistry();

    public void apply(Settings settings) {
        settings.getPlugins().apply("jvm-toolchain-management");
        JavaToolchainResolverRegistry registry = getToolchainResolverRegistry();
        registry.register(OracleOpenJdkToolchainResolver.class);
        registry.register(AdoptiumJdkToolchainResolver.class);
        registry.register(ArchivedOracleJdkToolchainResolver.class);
    }
}
