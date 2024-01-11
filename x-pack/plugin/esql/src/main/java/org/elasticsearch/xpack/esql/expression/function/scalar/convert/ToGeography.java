/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.GeographyValidator;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.io.IOException;
import java.nio.ByteOrder;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.type.EsqlDataTypes.GEOGRAPHY;
import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;

public class ToGeography extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(GEOGRAPHY, (fieldEval, source) -> fieldEval),
        Map.entry(KEYWORD, ToGeographyFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToGeographyFromStringEvaluator.Factory::new)
    );

    @FunctionInfo(returnType = "geography", description = "Converts an input value to a geography value.")
    public ToGeography(Source source, @Param(name = "v", type = { "geography", "keyword", "text" }) Expression field) {
        super(source, field);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return GEOGRAPHY;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToGeography(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToGeography::new, field());
    }

    @ConvertEvaluator(extraName = "FromString", warnExceptions = { IllegalArgumentException.class })
    static BytesRef fromKeyword(BytesRef in) {
        try {
            final Geometry geometry = WellKnownText.fromWKT(GeographyValidator.instance(true), false, in.utf8ToString());
            return new BytesRef(WellKnownBinary.toWKB(geometry, ByteOrder.LITTLE_ENDIAN));
        } catch (IOException | ParseException e) {
            throw new IllegalArgumentException("Failed to parse WKT: " + e.getMessage(), e);
        }
    }
}
