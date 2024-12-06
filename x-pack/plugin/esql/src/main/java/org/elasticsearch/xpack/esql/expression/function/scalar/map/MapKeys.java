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
import org.elasticsearch.xpack.esql.core.expression.NamedLiterals;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

public class MapKeys extends ScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "MapKeys", MapKeys::new);

    private final Expression map;

    @FunctionInfo(returnType = "keyword", description = "Return the keys of a map")
    public MapKeys(
        Source source,
        @MapParam(
            name = "map",
            keyType = "keyword",
            valueType = { "keyword", "integer", "double", "boolean" },
            description = "Input value. The input is a valid constant map expression."
        ) Expression v
    ) {
        super(source, Arrays.asList(v));
        this.map = v;
    }

    private MapKeys(StreamInput in) throws IOException {
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
        // namedLiterals does not have a type associated with it
        return isFoldable(map, sourceText(), FIRST);
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    public boolean foldable() {
        return map.foldable();
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new MapKeys(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, MapKeys::new, map);
    }

    @Override
    public Object fold() {
        if (map instanceof NamedLiterals nl) {
            return nl.args().keySet().stream().collect(Collectors.joining(", "));
        } else {
            throw new IllegalArgumentException(
                LoggerMessageFormat.format(null, "Invalid format for [{}], expect a map but got {}", sourceText(), map.fold())
            );
        }
    }
}
