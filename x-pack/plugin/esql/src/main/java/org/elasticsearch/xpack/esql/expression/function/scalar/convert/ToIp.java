/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.SurrogateExpression;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isMapExpression;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isTypeOrUnionType;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction.supportedTypesNames;

/**
 * Converts strings to IPs.
 * <p>
 *     IPv4 addresses have traditionally parsed quads with leading zeros in three
 *     mutually exclusive ways:
 * </p>
 * <ul>
 *     <li>As octal numbers. So {@code 1.1.010.1} becomes {@code 1.1.8.1}.</li>
 *     <li>As decimal numbers. So {@code 1.1.010.1} becomes {@code 1.1.10.1}.</li>
 *     <li>Rejects them entirely. So {@code 1.1.010.1} becomes {@code null} with a warning.</li>
 * </ul>
 * <p>
 *     These three ways of handling leading zeros are available with the optional
 *     {@code leading_zeros} named parameter. Set to {@code octal}, {@code decimal},
 *     or {@code reject}. If not sent this defaults to {@code reject} which has
 *     been Elasticsearch's traditional way of handling leading zeros for years.
 * </p>
 * <p>
 *     This doesn't extend from {@link AbstractConvertFunction} so that it can
 *     support a named parameter for the leading zeros behavior. Instead, it rewrites
 *     itself into either {@link ToIpLeadingZerosOctal}, {@link ToIpLeadingZerosDecimal},
 *     or {@link ToIpLeadingZerosRejected} which are all {@link AbstractConvertFunction}
 *     subclasses. This keeps the conversion code happy while still allowing us to
 *     expose a single method to users.
 * </p>
 */
public class ToIp extends EsqlScalarFunction implements SurrogateExpression, OptionalArgument, ConvertFunction {
    private static final String LEADING_ZEROS = "leading_zeros";
    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(Map.entry(LEADING_ZEROS, KEYWORD));

    private final Expression field;
    private final Expression options;

    @FunctionInfo(
        returnType = "ip",
        description = "Converts an input string to an IP value.",
        examples = {
            @Example(file = "ip", tag = "to_ip", explanation = """
                Note that in this example, the last conversion of the string isn’t possible.
                When this happens, the result is a `null` value. In this case a _Warning_ header is added to the response.
                The header will provide information on the source of the failure:

                `"Line 1:68: evaluation of [TO_IP(str2)] failed, treating result as null. Only first 20 failures recorded."`

                A following header will contain the failure reason and the offending value:

                `"java.lang.IllegalArgumentException: 'foo' is not an IP string literal."`"""),
            @Example(file = "ip", tag = "to_ip_leading_zeros_octal", explanation = """
                Parse v4 addresses with leading zeros as octal. Like `ping` or `ftp`.
                """),
            @Example(file = "ip", tag = "to_ip_leading_zeros_decimal", explanation = """
                Parse v4 addresses with leading zeros as decimal. Java's `InetAddress.getByName`.
                """) }
    )
    public ToIp(
        Source source,
        @Param(
            name = "field",
            type = { "ip", "keyword", "text" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "leading_zeros",
                    type = "keyword",
                    valueHint = { "reject", "octal", "decimal" },
                    description = "What to do with leading 0s in IPv4 addresses."
                ) },
            description = "(Optional) Additional options.",
            optional = true
        ) Expression options
    ) {
        super(source, options == null ? List.of(field) : List.of(field, options));
        this.field = field;
        this.options = options;
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("not serialized");
    }

    @Override
    public DataType dataType() {
        return IP;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToIp(source(), newChildren.get(0), newChildren.size() == 1 ? null : newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToIp::new, field, options);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        throw new UnsupportedOperationException("should be rewritten");
    }

    @Override
    public Expression surrogate() {
        return LeadingZeros.from((MapExpression) options).surrogate(source(), field);
    }

    @Override
    public Expression field() {
        return field;
    }

    @Override
    public Set<DataType> supportedTypes() {
        // All ToIpLeadingZeros* functions support the same input set. So we just pick one.
        return ToIpLeadingZerosRejected.EVALUATORS.keySet();
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        TypeResolution resolution = isTypeOrUnionType(
            field,
            ToIpLeadingZerosRejected.EVALUATORS::containsKey,
            sourceText(),
            null,
            supportedTypesNames(supportedTypes())
        );
        if (resolution.unresolved()) {
            return resolution;
        }
        if (options == null) {
            return resolution;
        }
        resolution = isMapExpression(options, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        for (EntryExpression e : ((MapExpression) options).entryExpressions()) {
            String key;
            if (e.key().dataType() != KEYWORD) {
                return new TypeResolution("map keys must be strings");
            }
            if (e.key() instanceof Literal keyl) {
                key = (String) keyl.value();
            } else {
                return new TypeResolution("map keys must be literals");
            }
            DataType expected = ALLOWED_OPTIONS.get(key);
            if (expected == null) {
                return new TypeResolution("[" + key + "] is not a supported option");
            }

            if (e.value().dataType() != expected) {
                return new TypeResolution("[" + key + "] expects [" + expected + "] but was [" + e.value().dataType() + "]");
            }
            if (e.value() instanceof Literal == false) {
                return new TypeResolution("map values must be literals");
            }
        }
        try {
            LeadingZeros.from((MapExpression) options);
        } catch (IllegalArgumentException e) {
            return new TypeResolution(e.getMessage());
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public enum LeadingZeros {
        REJECT {
            @Override
            public Expression surrogate(Source source, Expression field) {
                return new ToIpLeadingZerosRejected(source, field);
            }
        },
        DECIMAL {
            @Override
            public Expression surrogate(Source source, Expression field) {
                return new ToIpLeadingZerosDecimal(source, field);
            }
        },
        OCTAL {
            @Override
            public Expression surrogate(Source source, Expression field) {
                return new ToIpLeadingZerosOctal(source, field);
            }
        };

        public static LeadingZeros from(MapExpression exp) {
            if (exp == null) {
                return REJECT;
            }
            Expression e = exp.keyFoldedMap().get(LEADING_ZEROS);
            return e == null ? REJECT : from((String) ((Literal) e).value());
        }

        public static LeadingZeros from(String str) {
            return switch (str) {
                case "reject" -> REJECT;
                case "octal" -> OCTAL;
                case "decimal" -> DECIMAL;
                default -> throw new IllegalArgumentException("Illegal leading_zeros [" + str + "]");
            };
        }

        public abstract Expression surrogate(Source source, Expression field);
    }
}
