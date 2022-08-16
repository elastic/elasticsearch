/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.support;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;

public abstract class ShapeValuesSourceType implements Writeable, ValuesSourceType {

    @Override
    public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
        // TODO (support scripts)
        throw new UnsupportedOperationException(typeName());
    }

    @Override
    public abstract ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script, AggregationContext context);

    @Override
    public void writeTo(StreamOutput out) throws IOException {

    }
}
