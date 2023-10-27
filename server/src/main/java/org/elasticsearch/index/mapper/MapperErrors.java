/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

public class MapperErrors {
    static void throwObjectMappingConflictError(String fieldName) throws IllegalArgumentException {
        throw new IllegalArgumentException("can't merge a non object mapping [" + fieldName + "] with an object mapping");
    }

    static void throwNestedMappingConflictError(String fieldName) throws IllegalArgumentException {
        throw new IllegalArgumentException("can't merge a non-nested mapping [" + fieldName + "] with a nested mapping");
    }
}
