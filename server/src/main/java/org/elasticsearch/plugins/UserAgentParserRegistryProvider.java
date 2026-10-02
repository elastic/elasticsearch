/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.env.Environment;
import org.elasticsearch.useragent.api.UserAgentParserRegistry;

/**
 * SPI interface for plugins that provide a {@link UserAgentParserRegistry}.
 * Implemented by the user-agent module plugin; used by {@link org.elasticsearch.node.NodeConstruction}
 * to obtain the registry before {@link org.elasticsearch.ingest.IngestService} construction.
 */
public interface UserAgentParserRegistryProvider {

    /**
     * Creates a {@link UserAgentParserRegistry} for the given environment.
     */
    UserAgentParserRegistry createRegistry(Environment env);
}
