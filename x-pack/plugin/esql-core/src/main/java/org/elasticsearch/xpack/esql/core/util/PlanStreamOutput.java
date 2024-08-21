/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.io.IOException;
import java.util.Map;

public interface PlanStreamOutput {

    /**
     * Writes a cache header for an {@link Attribute} and caches it if it is not already in the cache.
     * In that case, the attribute will have to serialize itself into this stream immediately after this method call.
     * @param attribute The attribute to serialize
     * @return true if the attribute needs to serialize itself, false otherwise (ie. if already cached)
     * @throws IOException
     */
    boolean writeAttributeCacheHeader(Attribute attribute) throws IOException;

    static void writeEsField(PlanStreamOutput output, EsField field) throws IOException {
        output.writeEsField(field);
    }

    void writeEsField(EsField field) throws IOException;

    // TODO get rid of this method when we unify esql and esql-core
    <V> void writeMap(Map<String, V> map, Writeable.Writer<V> valueWriter) throws IOException;
}
