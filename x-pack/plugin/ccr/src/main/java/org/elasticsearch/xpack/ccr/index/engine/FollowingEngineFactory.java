/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;

/**
 * An engine factory for following engines.
 */
public final class FollowingEngineFactory implements EngineFactory {

    @Override
    public Engine newReadWriteEngine(final EngineConfig config) {
        return new FollowingEngine(config);
    }

}
