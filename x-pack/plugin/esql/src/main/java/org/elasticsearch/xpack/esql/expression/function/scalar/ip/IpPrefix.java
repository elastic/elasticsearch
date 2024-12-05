/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.EvalOperator.ExpressionEvaluator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isIPAndExact;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;

/**
 * Truncates an IP value to a given prefix length.
 */
public class IpPrefix extends EsqlScalarFunction implements OptionalArgument {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "IpPrefix", IpPrefix::new);

    // Borrowed from Lucene, rfc4291 prefix
    private static final byte[] IPV4_PREFIX = new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, -1, -1 };

    private final Expression ipField;
    private final Expression prefixLengthV4Field;
    private final Expression prefixLengthV6Field;

    @FunctionInfo(
        returnType = "ip",
        description = "Truncates an IP to a given prefix length.",
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
            description = "Prefix length for IPv4 addresses."
        ) Expression prefixLengthV4Field,
        @Param(
            name = "prefixLengthV6",
            type = { "integer" },
            description = "Prefix length for IPv6 addresses."
        ) Expression prefixLengthV6Field
    ) {
        super(source, Arrays.asList(ipField, prefixLengthV4Field, prefixLengthV6Field));
        this.ipField = ipField;
        this.prefixLengthV4Field = prefixLengthV4Field;
        this.prefixLengthV6Field = prefixLengthV6Field;
    }

    private IpPrefix(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(ipField);
        out.writeNamedWriteable(prefixLengthV4Field);
        out.writeNamedWriteable(prefixLengthV6Field);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var ipEvaluatorSupplier = toEvaluator.apply(ipField);
        var prefixLengthV4EvaluatorSupplier = toEvaluator.apply(prefixLengthV4Field);
        var prefixLengthV6EvaluatorSupplier = toEvaluator.apply(prefixLengthV6Field);

        return new IpPrefixEvaluator.Factory(
            source(),
            ipEvaluatorSupplier,
            prefixLengthV4EvaluatorSupplier,
            prefixLengthV6EvaluatorSupplier,
            context -> new BytesRef(new byte[16])
        );
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    static BytesRef process(
        BytesRef ip,
        int prefixLengthV4,
        int prefixLengthV6,
        @Fixed(includeInToString = false, build = true) BytesRef scratch
    ) {
        if (prefixLengthV4 < 0 || prefixLengthV4 > 32) {
            throw new IllegalArgumentException("Prefix length v4 must be in range [0, 32], found " + prefixLengthV4);
        }
        if (prefixLengthV6 < 0 || prefixLengthV6 > 128) {
            throw new IllegalArgumentException("Prefix length v6 must be in range [0, 128], found " + prefixLengthV6);
        }

        boolean isIpv4 = Arrays.compareUnsigned(
            ip.bytes,
            ip.offset,
            ip.offset + IPV4_PREFIX.length,
            IPV4_PREFIX,
            0,
            IPV4_PREFIX.length
        ) == 0;

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
            scratch.bytes[fullBytes] = (byte) (ip.bytes[ip.offset + fullBytes] & lastByteMask);
        }

        // Copy the last empty bytes
        if (fullBytes < 16) {
            Arrays.fill(scratch.bytes, fullBytes + 1, 16, (byte) 0);
        }
    }

    @Override
    public DataType dataType() {
        return DataType.IP;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isIPAndExact(ipField, sourceText(), FIRST).and(
            isType(prefixLengthV4Field, dt -> dt == INTEGER, sourceText(), SECOND, "integer")
        ).and(isType(prefixLengthV6Field, dt -> dt == INTEGER, sourceText(), THIRD, "integer"));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new IpPrefix(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, IpPrefix::new, ipField, prefixLengthV4Field, prefixLengthV6Field);
    }
}
