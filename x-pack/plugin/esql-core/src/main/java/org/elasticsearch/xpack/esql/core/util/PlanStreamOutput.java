/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

public interface PlanStreamOutput {

    byte NEW = 0;
    byte CACHED = 1;
    byte NO_CACHE = 2;

    /**
     * Retrieves the ID for a FieldAttribute from the cache.
     * It is just a serialization ID, valid only for the single plan serialization.
     * (For clarity, it has nothing to do with {@link org.elasticsearch.xpack.esql.core.expression.NameId}s.)
     * @param attr the attribute to be serialized
     * @return the serialization ID if the attribute was already cached;
     * null otherwise
     */
    Integer attributeIdFromCache(Attribute attr);

    /**
     * Adds a FieldAttribute to the cache, returning the new serialization ID assigned to it.
     * @return the cache ID; or null if the attribute cannot be added to the cache
     * @throws IllegalArgumentException if the attribute is already present in the cache
     */
    Integer cacheAttribute(Attribute attr);
}
