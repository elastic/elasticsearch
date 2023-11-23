/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.versionfield.Version;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.ql.type.DataTypes.KEYWORD;
import static org.elasticsearch.xpack.ql.type.DataTypes.TEXT;
import static org.elasticsearch.xpack.ql.type.DataTypes.VERSION;

public class ToVersion extends AbstractConvertFunction {

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(VERSION, (fieldEval, source) -> fieldEval),
        Map.entry(KEYWORD, ToVersionFromStringEvaluator.Factory::new),
        Map.entry(TEXT, ToVersionFromStringEvaluator.Factory::new)
    );

    public ToVersion(Source source, @Param(name = "v", type = { "keyword", "text", "version" }) Expression v) {
        super(source, v);
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return EVALUATORS;
    }

    @Override
    public DataType dataType() {
        return VERSION;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToVersion(source(), newChildren.get(0));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToVersion::new, field());
    }

    @ConvertEvaluator(extraName = "FromString")
    static BytesRef fromKeyword(BytesRef asString) {
        return new Version(asString.utf8ToString()).toBytesRef();
    }
}
