/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.index.codec.CodecProvider;
import org.elasticsearch.index.engine.EngineConfig;

/**
 * SPI for plugins extending the stateless plugin to supply a custom {@link CodecProvider} per engine.
 * When no implementation is loaded, the engine's codec provider is used unchanged.
 */
public interface CodecProviderFactory {

    /**
     * Returns the codec provider to use for the given engine configuration.
     *
     * @param engineConfig the engine configuration
     * @return the codec provider
     */
    CodecProvider getCodecProvider(EngineConfig engineConfig);
}
