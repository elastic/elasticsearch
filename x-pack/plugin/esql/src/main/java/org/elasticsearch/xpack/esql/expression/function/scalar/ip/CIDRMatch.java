/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.network.CIDRUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.planner.Mappable;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.fromIndex;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isIPAndExact;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

/**
 * This function takes a first parameter of type IP, followed by one or more parameters evaluated to a CIDR specification:
 * <ul>
 * <li>a string literal;</li>
 * <li>a field of type keyword;</li>
 * <li>a function outputting a keyword.</li>
 * </ul><p>
 * The function will match if the IP parameter is within any (not all) of the ranges defined by the provided CIDR specs.
 * <p>
 * Example: `| eval cidr="10.0.0.0/8" | where cidr_match(ip_field, "127.0.0.1/30", cidr)`
 */
public class CIDRMatch extends ScalarFunction implements Mappable {

    private final Expression ipField;
    private final List<Expression> matches;

    public CIDRMatch(Source source, Expression ipField, List<Expression> matches) {
        super(source, CollectionUtils.combine(singletonList(ipField), matches));
        this.ipField = ipField;
        this.matches = matches;
    }

    @Override
    public Supplier<EvalOperator.ExpressionEvaluator> toEvaluator(
        Function<Expression, Supplier<EvalOperator.ExpressionEvaluator>> toEvaluator
    ) {
        Supplier<EvalOperator.ExpressionEvaluator> ipEvaluatorSupplier = toEvaluator.apply(ipField);
        return () -> new CIDRMatchEvaluator(
            ipEvaluatorSupplier.get(),
            matches.stream().map(x -> toEvaluator.apply(x).get()).toArray(EvalOperator.ExpressionEvaluator[]::new)
        );
    }

    @Evaluator
    static boolean process(BytesRef ip, BytesRef[] cidrs) {
        for (var cidr : cidrs) {
            // simple copy is safe, Java uses big-endian, same as network order
            if (CIDRUtils.isInRange(Arrays.copyOfRange(ip.bytes, ip.offset, ip.offset + ip.length), cidr.utf8ToString())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public DataType dataType() {
        return DataTypes.BOOLEAN;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isIPAndExact(ipField, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        int i = 1;
        for (var m : matches) {
            resolution = isStringAndExact(m, sourceText(), fromIndex(i++));
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return resolution;
    }

    @Override
    public ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new CIDRMatch(source(), newChildren.get(0), newChildren.subList(1, newChildren.size()));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, CIDRMatch::new, children().get(0), children().subList(1, children().size()));
    }
}
