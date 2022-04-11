/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.common.transport.TransportAddress;

import java.util.List;
import java.util.function.Consumer;

public interface ConfiguredHostsResolver {
    /**
     * Attempt to resolve the configured hosts list to a list of transport addresses.
     *
     * @param consumer Consumer for the resolved list. May not be called if an error occurs or if another resolution attempt is in progress.
     */
    void resolveConfiguredHosts(Consumer<List<TransportAddress>> consumer);
}
