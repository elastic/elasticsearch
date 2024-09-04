/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.io.IOException;

public interface PlanStreamOutput {

    /**
     * Writes a cache header for an {@link Attribute} and caches it if it is not already in the cache.
     * In that case, the attribute will have to serialize itself into this stream immediately after this method call.
     * @param attribute The attribute to serialize
     * @return true if the attribute needs to serialize itself, false otherwise (ie. if already cached)
     * @throws IOException
     */
    boolean writeAttributeCacheHeader(Attribute attribute) throws IOException;

    /**
     * Writes a cache header for an {@link org.elasticsearch.xpack.esql.core.type.EsField} and caches it if it is not already in the cache.
     * In that case, the field will have to serialize itself into this stream immediately after this method call.
     * @param field The EsField to serialize
     * @return true if the attribute needs to serialize itself, false otherwise (ie. if already cached)
     * @throws IOException
     */
    boolean writeEsFieldCacheHeader(EsField field) throws IOException;
}
