/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.DataTypes;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isIPAndExact;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataTypes.INTEGER;

/**
 * Truncates an IP value to a given prefix length.
 */
public class IpPrefix extends EsqlScalarFunction {
    // Borrowed from Lucene, rfc4291 prefix
    private static final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };

    private final Expression ipField;
    private final Expression prefixLengthV4Field;
    private final Expression prefixLengthV6Field;

    @FunctionInfo(
        returnType = "ip",
        description = "Returns the original IP with all but the first N bits set to zero.",
        examples = @Example(file = "ip", tag = "ipPrefix")
    )
    public IpPrefix(
        Source source,
        @Param(
            name = "ip",
            type = { "ip" },
            description = "IP address of type `ip` (both IPv4 and IPv6 are supported)."
        ) Expression ipField,
        @Param(
            name = "prefixLengthV4",
            type = { "integer" },
            description = "Prefix length, in the range [0, 32], to apply to IPv4 addresses."
        ) Expression prefixLengthV4Field,
        @Param(
            name = "prefixLengthV6",
            type = { "integer" },
            optional = true,
            description = "Prefix length, in the range [0, 128], to apply to IPv6 addresses. "
                + "If not provided, the original IPv6 addresses will be returned."
        ) Expression prefixLengthV6Field
    ) {
        super(
            source,
            prefixLengthV6Field == null
                ? Arrays.asList(ipField, prefixLengthV4Field)
                : Arrays.asList(ipField, prefixLengthV4Field, prefixLengthV6Field)
        );
        this.ipField = ipField;
        this.prefixLengthV4Field = prefixLengthV4Field;
        this.prefixLengthV6Field = prefixLengthV6Field;
    }

    public static IpPrefix readFrom(PlanStreamInput in) throws IOException {
        return new IpPrefix(in.readSource(), in.readExpression(), in.readExpression(), in.readOptionalNamed(Expression.class));
    }

    public static void writeTo(PlanStreamOutput out, IpPrefix ipPrefix) throws IOException {
        out.writeSource(ipPrefix.source());
        List<Expression> fields = ipPrefix.children();
        assert fields.size() == 2 || fields.size() == 3;
        out.writeExpression(fields.get(0));
        out.writeExpression(fields.get(1));
        out.writeOptionalWriteable(fields.size() == 3 ? o -> out.writeExpression(fields.get(2)) : null);
    }

    public Expression ipField() {
        return ipField;
    }

    public Expression prefixLengthV4Field() {
        return prefixLengthV4Field;
    }

    public Expression prefixLengthV6Field() {
        return prefixLengthV6Field;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var ipEvaluatorSupplier = toEvaluator.apply(ipField);
        var prefixLengthV4EvaluatorSupplier = toEvaluator.apply(prefixLengthV4Field);

        if (prefixLengthV6Field != null) {
            var prefixLengthV6EvaluatorSupplier = toEvaluator.apply(prefixLengthV6Field);

            return new IpPrefixEvaluator.Factory(
                source(),
                ipEvaluatorSupplier,
                prefixLengthV4EvaluatorSupplier,
                prefixLengthV6EvaluatorSupplier,
                context -> new BytesRef(new byte[16])
            );
        }

        return new IpPrefixOnlyV4Evaluator.Factory(
            source(),
            ipEvaluatorSupplier,
            prefixLengthV4EvaluatorSupplier,
            context -> new BytesRef(new byte[16])
        );
    }

    @Evaluator(extraName = "OnlyV4")
    static BytesRef process(BytesRef ip, int prefixLengthV4, @Fixed(includeInToString = false, build = true) BytesRef scratch) {
        return process(ip, prefixLengthV4, 128, scratch);
    }

    @Evaluator
    static BytesRef process(
        BytesRef ip,
        int prefixLengthV4,
        int prefixLengthV6,
        @Fixed(includeInToString = false, build = true) BytesRef scratch
    ) {
        if (ip.length != 16 || prefixLengthV4 < 0 || prefixLengthV4 > 32 || prefixLengthV6 < 0 || prefixLengthV6 > 128) {
            return null;
        }

        boolean isIpv4 = Arrays.compareUnsigned(ip.bytes, 0, IPV4_PREFIX.length, IPV4_PREFIX, 0, IPV4_PREFIX.length) == 0;

        if (isIpv4) {
            makePrefix(ip, scratch, 12 + prefixLengthV4 / 8, prefixLengthV4 % 8);
        } else {
            makePrefix(ip, scratch, prefixLengthV6 / 8, prefixLengthV6 % 8);
        }

        return scratch;
    }

    private static void makePrefix(BytesRef ip, BytesRef scratch, int fullBytes, int remainingBits) {
        // Copy the first full bytes
        System.arraycopy(ip.bytes, ip.offset, scratch.bytes, 0, fullBytes);

        // Copy the last byte ignoring the trailing bits
        if (remainingBits > 0) {
            byte lastByteMask = (byte) (0xFF << (8 - remainingBits));
            scratch.bytes[fullBytes] = (byte) (ip.bytes[fullBytes] & lastByteMask);
        }

        // Copy the last empty bytes
        if (fullBytes < 16) {
            Arrays.fill(scratch.bytes, fullBytes + 1, 16, (byte) 0);
        }
    }

    @Override
    public DataType dataType() {
        return DataTypes.IP;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution typeResolution = isIPAndExact(ipField, sourceText(), FIRST).and(
            isType(prefixLengthV4Field, dt -> dt == INTEGER, sourceText(), SECOND, "integer")
        );

        if (prefixLengthV6Field != null) {
            typeResolution = typeResolution.and(isType(prefixLengthV6Field, dt -> dt == INTEGER, sourceText(), THIRD, "integer"));
        }

        return typeResolution;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new IpPrefix(source(), newChildren.get(0), newChildren.get(1), newChildren.size() == 3 ? newChildren.get(2) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, IpPrefix::new, ipField, prefixLengthV4Field, prefixLengthV6Field);
    }
}
