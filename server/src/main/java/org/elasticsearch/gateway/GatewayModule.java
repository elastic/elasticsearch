/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.elasticsearch.common.inject.AbstractModule;

public class GatewayModule extends AbstractModule {


    @Override
    protected void configure() {
        bind(DanglingIndicesState.class).asEagerSingleton();
        bind(GatewayService.class).asEagerSingleton();
        bind(LocalAllocateDangledIndices.class).asEagerSingleton();
    }
}
