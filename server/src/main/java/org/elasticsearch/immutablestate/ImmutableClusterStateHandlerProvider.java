/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.immutablestate;

import java.util.Collection;

/**
 * SPI service interface for supplying {@link ImmutableClusterStateHandler} implementations to Elasticsearch
 * from plugins/modules.
 */
public interface ImmutableClusterStateHandlerProvider {
    /**
     * Returns a list of {@link ImmutableClusterStateHandler} implementations that a module/plugin supplies.
     * @see ImmutableClusterStateHandler
     *
     * @return a list of ${@link ImmutableClusterStateHandler}s
     */
    Collection<ImmutableClusterStateHandler<?>> handlers();
}
