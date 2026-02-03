/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.config;

import org.elasticsearch.entitlement.runtime.registry.InternalInstrumentationRegistry;

import java.util.List;

public class MainInstrumentationProvider implements InstrumentationConfig {
    @Override
    public void init(InternalInstrumentationRegistry registry) {
        List.of(
            new ClassLoaderInstrumentation(),
            new FileInstrumentation(),
            new FileStoreInstrumentation(),
            new FileSystemProviderInstrumentation(),
            new L10nInstrumentation(),
            new NetworkInstrumentation(),
            new PathInstrumentation(),
            new SecurityInstrumentation(),
            new SelectorProviderInstrumentation(),
            new SystemInstrumentation(),
            new ThreadInstrumentation()
        ).forEach(config -> config.init(registry));
    }
}
