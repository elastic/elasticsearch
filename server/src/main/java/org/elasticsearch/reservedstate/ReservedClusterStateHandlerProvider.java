/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate;

import java.util.Collection;

/**
 * SPI service interface for supplying {@link ReservedClusterStateHandler} implementations to Elasticsearch
 * from plugins/modules.
 */
public interface ReservedClusterStateHandlerProvider {
    /**
     * Returns a list of {@link ReservedClusterStateHandler} implementations that a module/plugin supplies.
     * @see ReservedClusterStateHandler
     *
     * @return a list of ${@link ReservedClusterStateHandler}s
     */
    Collection<ReservedClusterStateHandler<?>> handlers();
}
