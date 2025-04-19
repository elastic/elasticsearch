/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.h3.H3;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;

public class StGeohexToLong extends AbstractConvertFunction implements EvaluatorMapper {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "StGeohexToLong",
        StGeohexToLong::new
    );

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(LONG, (source, fieldEval) -> fieldEval),
        Map.entry(KEYWORD, StGeohexToLongFromStringEvaluator.Factory::new)
    );

    @FunctionInfo(
        returnType = "long",
        description = "Converts an input value representing a geohex grid-ID in string format into a long.",
        examples = { @Example(file = "spatial-grid", tag = "geohex_to_long") },
        depthOffset = 1 // make it appear as a subsection of ST_GEOHEX
    )
    public StGeohexToLong(
        Source source,
        @Param(
            name = "grid_id",
            type = { "keyword", "long" },
            description = "Input geohex grid-id. The input can be a single- or multi-valued column or an expression."
        ) Expression v
    ) {
        super(source, v);
    }

    private StGeohexToLong(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return LONG;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new StGeohexToLong(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, StGeohexToLong::new, field());
    }

    @ConvertEvaluator(extraName = "FromString")
    static long fromString(BytesRef gridId) {
        return H3.stringToH3(gridId.utf8ToString());
    }
}
