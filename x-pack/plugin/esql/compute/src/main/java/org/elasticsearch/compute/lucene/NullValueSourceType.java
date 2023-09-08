/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene;

import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.FieldContext;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public class NullValueSourceType implements ValuesSourceType {

    @Override
    public ValuesSource getEmpty() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ValuesSource replaceMissing(
        ValuesSource valuesSource,
        Object rawMissing,
        DocValueFormat docValueFormat,
        AggregationContext context
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String typeName() {
        return null;
    }

}
