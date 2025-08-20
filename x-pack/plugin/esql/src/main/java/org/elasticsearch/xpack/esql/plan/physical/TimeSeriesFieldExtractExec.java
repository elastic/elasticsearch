/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TimeSeriesFieldExtractExec extends FieldExtractExec {
    public TimeSeriesFieldExtractExec(
        Source source,
        PhysicalPlan child,
        List<Attribute> attributesToExtract,
        MappedFieldType.FieldExtractPreference defaultPreference,
        Set<Attribute> docValuesAttributes,
        Set<Attribute> boundsAttributes
    ) {
        super(source, child, attributesToExtract, defaultPreference, docValuesAttributes, boundsAttributes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("local plan");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("local plan");
    }

    @Override
    protected NodeInfo<TimeSeriesFieldExtractExec> info() {
        return NodeInfo.create(
            this,
            TimeSeriesFieldExtractExec::new,
            child(),
            attributesToExtract,
            defaultPreference,
            docValuesAttributes,
            boundsAttributes
        );
    }

    @Override
    public UnaryExec replaceChild(PhysicalPlan newChild) {
        return new TimeSeriesFieldExtractExec(
            source(),
            newChild,
            attributesToExtract,
            defaultPreference,
            docValuesAttributes,
            boundsAttributes
        );
    }

    @Override
    public FieldExtractExec withDocValuesAttributes(Set<Attribute> docValuesAttributes) {
        return new TimeSeriesFieldExtractExec(
            source(),
            child(),
            attributesToExtract,
            defaultPreference,
            docValuesAttributes,
            boundsAttributes
        );
    }

    @Override
    public FieldExtractExec withBoundsAttributes(Set<Attribute> boundsAttributes) {
        return new TimeSeriesFieldExtractExec(
            source(),
            child(),
            attributesToExtract,
            defaultPreference,
            docValuesAttributes,
            boundsAttributes
        );
    }
}
