/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.OBJECT;

/**
 * A field mapped as the Elasticsearch {@code nested} type: an array of objects whose sub-fields stay
 * associated with the object they belong to (unlike a plain {@code object}, which flattens into
 * independent multi-valued leaves).
 * <p>
 * A nested field is deliberately <b>not</b> flattened into queryable attributes by
 * {@code Analyzer.mappingAsAttributes} — its sub-fields remain in {@link #getProperties()} and are only
 * reachable inside a {@code NESTED_ANY} predicate (and, later, after {@code NESTED_EXPAND}). Because the
 * container itself is never emitted as a column, a bare reference to it fails resolution like any unknown
 * column. Its {@link DataType} is {@link DataType#OBJECT}, so it is already non-primitive and
 * non-representable; the nested-vs-object distinction is carried by this class, not by a dedicated type.
 * <p>
 * Note: {@code withDataType} is not overridden, so re-typing a nested field would fall back to a plain
 * {@link EsField} and lose the nested identity. That is never done today (the analyzer skips nested fields
 * before any widening), but a future caller that re-types nested fields must account for it.
 */
public class NestedEsField extends EsField {

    public NestedEsField(String name, Map<String, EsField> properties, boolean isAlias, TimeSeriesFieldType timeSeriesFieldType) {
        super(name, OBJECT, properties, false, isAlias, timeSeriesFieldType);
    }

    public NestedEsField(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "NestedEsField";
    }
}
