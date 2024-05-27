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

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isIPAndExact;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isInteger;

/**
 * Converts an IP value and a prefix length to the prefix.
 */
public class IpPrefix extends EsqlScalarFunction {

    private final Expression ipField;
    private final Expression prefixLengthField;

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
            name = "prefixLength",
            type = { "integer" },
            description = "Prefix length. If in the range (0, 32), the IP is treated as an IPv4 address. "
                + "If in the range (32, 128), the IP is treated as an IPv6 address. "
                + "If the prefix length is out of range, the function returns `null`."
        ) Expression prefixLengthField
    ) {
        super(source, Arrays.asList(ipField, prefixLengthField));
        this.ipField = ipField;
        this.prefixLengthField = prefixLengthField;
    }

    public Expression ipField() {
        return ipField;
    }

    public Expression prefixLengthField() {
        return prefixLengthField;
    }

    @Override
    public boolean foldable() {
        return Expressions.foldable(children());
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(Function<Expression, ExpressionEvaluator.Factory> toEvaluator) {
        var ipEvaluatorSupplier = toEvaluator.apply(ipField);
        var prefixLengthEvaluatorSupplier = toEvaluator.apply(prefixLengthField);
        return new IpPrefixEvaluator.Factory(
            source(),
            ipEvaluatorSupplier,
            prefixLengthEvaluatorSupplier,
            context -> new BytesRef(new byte[16])
        );
    }

    @Evaluator
    static BytesRef process(BytesRef ip, int prefixLength, @Fixed(includeInToString = false, build = true) BytesRef scratch) {
        if (ip.length != 16 || prefixLength < 0 || prefixLength > ip.length * 8) {
            return null;
        }

        boolean isIpv4 = prefixLength <= 32;
        // IPv4 is stored in the last 4 bytes
        int offset = isIpv4 ? 12 : 0;
        int fullBytes = offset + prefixLength / 8;
        int remainingBits = prefixLength % 8;

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

        return scratch;
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

        TypeResolution ipResolution = isIPAndExact(ipField, sourceText(), FIRST);
        if (ipResolution.unresolved()) {
            return ipResolution;
        }

        return isInteger(prefixLengthField, sourceText(), SECOND);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new IpPrefix(source(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, IpPrefix::new, children().get(0), children().get(1));
    }
}
