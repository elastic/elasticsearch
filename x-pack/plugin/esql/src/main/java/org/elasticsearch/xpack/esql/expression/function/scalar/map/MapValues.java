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
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public class MapValues extends ScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "MapValues",
        MapValues::new
    );

    private final Map<String, String> map;

    @FunctionInfo(returnType = "long", description = "Return the values in a map")
    public MapValues(
        Source source,
        @MapParam(
            name = "map",
            keyType = "keyword",
            valueType = { "keyword", "integer", "double", "boolean" },
            description = "Input value. The input is a valid constant map expression."
        ) Map<String, String> map
    ) {
        super(source);
        this.map = map;
    }

    private MapValues(StreamInput in) throws IOException {
        this(Source.readFrom((PlanStreamInput) in), in.readMap(StreamInput::readString, StreamInput::readString));
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeMap(map, StreamOutput::writeString, StreamOutput::writeString);
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
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    public boolean foldable() {
        return true;
    }

    public Map<String, String> map() {
        return map;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return this;
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MapValues::new, map);
    }

    @Override
    public Object fold() {
        return map.values().stream().collect(Collectors.joining(", "));
    }
}
