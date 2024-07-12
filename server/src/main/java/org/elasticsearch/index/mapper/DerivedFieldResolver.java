/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.annotation.PublicApi;

import java.util.Set;

@PublicApi(since = "2.15.0")
public abstract class DerivedFieldResolver {
    /**
     * Resolves all derived fields matching a given pattern. It includes derived fields defined both in search requests
     * and index mapping.
     * @param pattern regex pattern
     * @return all derived fields matching the pattern
     */
    public abstract Set<String> resolvePattern(String pattern);

    /**
     * Resolves the MappedFieldType associated with a derived field
     * @param fieldName field name to lookup
     * @return mapped field type
     */
    public abstract MappedFieldType resolve(String fieldName);

}
