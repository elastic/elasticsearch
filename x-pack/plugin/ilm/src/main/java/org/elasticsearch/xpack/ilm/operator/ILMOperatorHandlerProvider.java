/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.operator;

import org.elasticsearch.operator.OperatorHandler;
import org.elasticsearch.operator.OperatorHandlerProvider;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ILM Provider implementation for the OperatorHandlerProvider service interface
 */
public class ILMOperatorHandlerProvider implements OperatorHandlerProvider {
    private static final Set<OperatorHandler<?>> handlers = ConcurrentHashMap.newKeySet();

    @Override
    public Collection<OperatorHandler<?>> handlers() {
        return handlers;
    }

    public static void handler(OperatorHandler<?> handler) {
        handlers.add(handler);
    }
}
