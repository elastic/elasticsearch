/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.map;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isMapExpression;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

public class MapCount extends ScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MapCount", MapCount::new);

    private final Expression map;

    @FunctionInfo(returnType = "long", description = "Count the number of entries in a map")
    public MapCount(
        Source source,
        @MapParam(
            name = "map",
            paramHint = { @MapParam.MapEntry(key = "option1", value = "value1"), @MapParam.MapEntry(key = "option2", value = "value2") },
            description = "Input value. The input is a valid constant map expression."
        ) Expression v
    ) {
        super(source, Collections.singletonList(v));
        this.map = v;
    }

    private MapCount(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readNamedWriteable(Expression.class));
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(map);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        // MapExpression does not have a DataType associated with it
        return isMapExpression(map, sourceText(), DEFAULT).and(isFoldable(map, sourceText(), DEFAULT));
    }

    @Override
    public DataType dataType() {
        return LONG;
    }

    @Override
    public boolean foldable() {
        return map.foldable();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MapCount(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MapCount::new, map);
    }

    @Override
    public Object fold() {
        if (map instanceof MapExpression me) {
            return (long) me.entries().size();
        } else {
            throw new IllegalArgumentException(
                LoggerMessageFormat.format(
                    null,
                    "Invalid format for [{}], expect a map of constant values but got {}",
                    sourceText(),
                    map.fold()
                )
            );
        }
    }

    public Expression map() {
        return this.map;
    }
}
