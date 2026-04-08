/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.xpack.esql.action.EsqlCursor;

import java.util.Map;

/**
 * Registry that holds all {@link CursorStorageService} implementations and resolves
 * the correct one based on {@link EsqlCursor.StorageType}.
 */
public class CursorStorageResolver {

    private final Map<EsqlCursor.StorageType, CursorStorageService> services;

    public CursorStorageResolver(Map<EsqlCursor.StorageType, CursorStorageService> services) {
        this.services = Map.copyOf(services);
    }

    public CursorStorageService resolve(EsqlCursor.StorageType type) {
        CursorStorageService service = services.get(type);
        if (service == null) {
            throw new IllegalArgumentException("no cursor storage service registered for type [" + type + "]");
        }
        return service;
    }
}
