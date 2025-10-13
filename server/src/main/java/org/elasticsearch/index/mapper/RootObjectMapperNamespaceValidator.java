/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.Nullable;

/**
 * SPI to inject additional rules around namespaces (top level fields) that are prohibited
 * in Elasticsearch mappings.
 */
public interface RootObjectMapperNamespaceValidator {
    /**
     * If the namespace in the mapper is not allowed, an Exception should be thrown.
     * @param subobjects Whether subobjects are enabled. Null is allowed
     * @param name namespace (field name) to validate
     */
    void validateNamespace(@Nullable ObjectMapper.Subobjects subobjects, String name);
}
