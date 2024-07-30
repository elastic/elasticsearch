/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;

public interface PlanStreamOutput {

    /**
     * Retrieves the ID for a FieldAttribute from the case.
     * It is just a serialization ID, valid only for the single plan serialization.
     * (for clarity, it has nothing to do NameID)
     * @param attr the attribute to be serialized
     * @return the serialization ID if the attribute was already cached;
     * null otherwise
     */
    Integer fromCache(FieldAttribute attr);

    /**
     * adds a FieldAttribute to the cache, returning the new serialization ID assigned to it.
     * @param attr
     * @return
     */
    Integer addToCache(FieldAttribute attr);
}
