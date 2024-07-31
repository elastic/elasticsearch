/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;

public interface PlanStreamOutput {

    /**
     * Writes cache header for {@link Attribute}s. It also handles the cache itself.
     * After this, the Attribute will also have to serialize itself
     * @param attribute The attribute to serialize
     * @return true if the attribute needs to serialize itself, false otherwise (ie. if already cached)
     * @throws IOException
     */
    boolean writeAttributeCacheHeader(Attribute attribute) throws IOException;
}
