/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isSpatial;

/**
 * A base class for spatial functions that operate on a single geometry input and produce a geometry output,
 * and can receive the incoming geometries from doc-values.
 */
public abstract class SpatialUnaryDocValuesFunction extends SpatialDocValuesFunction {
    protected SpatialUnaryDocValuesFunction(Source source, Expression spatialField, boolean spatialDocValues) {
        super(source, List.of(spatialField), spatialDocValues);
    }

    protected SpatialUnaryDocValuesFunction(StreamInput in) throws IOException {
        this(Source.readFrom((StreamInput & PlanStreamInput) in), in.readNamedWriteable(Expression.class), false);
    }

    @Override
    protected TypeResolution resolveType() {
        return isSpatial(spatialField(), sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(spatialField());
    }

    @Override
    public final Expression spatialField() {
        return children().getFirst();
    }
}
