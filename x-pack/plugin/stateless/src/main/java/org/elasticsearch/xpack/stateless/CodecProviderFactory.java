/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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
