/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.operator;

import java.util.Collection;

/**
 * SPI service interface for supplying OperatorHandler implementations to Elasticsearch
 * from plugins/modules.
 */
public interface OperatorHandlerProvider {
    /**
     * Returns a list of OperatorHandler implementations that a module/plugin supplies.
     * @see OperatorHandler
     *
     * @return a list of ${@link OperatorHandler}s
     */
    Collection<OperatorHandler<?>> handlers();
}
