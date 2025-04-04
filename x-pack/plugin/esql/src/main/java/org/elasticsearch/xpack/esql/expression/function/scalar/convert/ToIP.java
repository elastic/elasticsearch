/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.ConvertEvaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;
import org.elasticsearch.xpack.esql.core.expression.EntryExpression;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.Param;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.TransportVersions.ESQL_TO_IP_OPTIONS;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isMapExpression;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.type.DataType.IP;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.SEMANTIC_TEXT;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

public class ToIP extends AbstractConvertFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToIP", ToIP::new);

    private static final BytesRef LEADING_ZEROS = new BytesRef("leading_zeros");
    public static final Map<BytesRef, DataType> ALLOWED_OPTIONS = Map.ofEntries(Map.entry(LEADING_ZEROS, KEYWORD));

    private final Expression options;

    @FunctionInfo(
        returnType = "ip",
        description = "Converts an input string to an IP value.",
        examples = @Example(file = "ip", tag = "to_ip", explanation = """
            Note that in this example, the last conversion of the string isn’t possible.
            When this happens, the result is a `null` value. In this case a _Warning_ header is added to the response.
            The header will provide information on the source of the failure:

            `"Line 1:68: evaluation of [TO_IP(str2)] failed, treating result as null. Only first 20 failures recorded."`

            A following header will contain the failure reason and the offending value:

            `"java.lang.IllegalArgumentException: 'foo' is not an IP string literal."`""")
    )
    public ToIP(
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
        super(source, field);
        this.options = options;
    }

    private ToIP(StreamInput in) throws IOException {
        super(in);
        options = in.getTransportVersion().onOrAfter(ESQL_TO_IP_OPTIONS) ? in.readOptionalNamedWriteable(Expression.class) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().onOrAfter(ESQL_TO_IP_OPTIONS)) {
            out.writeOptionalNamedWriteable(options);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return LeadingZeros.from((MapExpression) options).evaluators;
    }

    @Override
    protected TypeResolution resolveType() {
        return resolveOptions().and(super.resolveType());
    }

    private TypeResolution resolveOptions() {
        if (options == null) {
            return TypeResolution.TYPE_RESOLVED;
        }
        TypeResolution resolution = isNotNull(options, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        // MapExpression does not have a DataType associated with it
        resolution = isMapExpression(options, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        for (EntryExpression e : ((MapExpression) options).entryExpressions()) {
            BytesRef key;
            if (e.key().dataType() != KEYWORD) {
                return new TypeResolution("map keys must be strings");
            }
            if (e.key() instanceof Literal keyl) {
                key = (BytesRef) keyl.value();
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
        REJECT(
            Map.ofEntries(
                Map.entry(IP, (source, field) -> field),
                Map.entry(KEYWORD, FROM_KEYWORD_LEADING_ZEROS_REJECTED),
                Map.entry(TEXT, FROM_KEYWORD_LEADING_ZEROS_REJECTED),
                Map.entry(SEMANTIC_TEXT, FROM_KEYWORD_LEADING_ZEROS_REJECTED)
            )
        ),
        DECIMAL(
            Map.ofEntries(
                Map.entry(IP, (source, field) -> field),
                Map.entry(KEYWORD, FROM_KEYWORD_LEADING_ZEROS_ARE_DECIMAL),
                Map.entry(TEXT, FROM_KEYWORD_LEADING_ZEROS_ARE_DECIMAL),
                Map.entry(SEMANTIC_TEXT, FROM_KEYWORD_LEADING_ZEROS_ARE_DECIMAL)
            )
        ),
        OCTAL(
            Map.ofEntries(
                Map.entry(IP, (source, field) -> field),
                Map.entry(KEYWORD, FROM_KEYWORD_LEADING_ZEROS_ARE_OCTAL),
                Map.entry(TEXT, FROM_KEYWORD_LEADING_ZEROS_ARE_OCTAL),
                Map.entry(SEMANTIC_TEXT, FROM_KEYWORD_LEADING_ZEROS_ARE_OCTAL)
            )
        );

        private final Map<DataType, BuildFactory> evaluators;

        LeadingZeros(Map<DataType, BuildFactory> evaluators) {
            this.evaluators = evaluators;
        }

        public static LeadingZeros from(MapExpression exp) {
            if (exp == null) {
                return REJECT;
            }
            Expression e = exp.keyFoldedMap().get(LEADING_ZEROS);
            return e == null ? REJECT : from(((BytesRef) ((Literal) e).value()).utf8ToString());
        }

        public static LeadingZeros from(String str) {
            return switch (str) {
                case "reject" -> REJECT;
                case "octal" -> OCTAL;
                case "decimal" -> DECIMAL;
                default -> throw new IllegalArgumentException("Illegal leading_zeros [" + str + "]");
            };
        }
    }

    @Override
    public DataType dataType() {
        return IP;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToIP(source(), newChildren.get(0), newChildren.size() == 1 ? null : newChildren.get(1));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToIP::new, field(), options);
    }

    private static final BuildFactory FROM_KEYWORD_LEADING_ZEROS_REJECTED = (
        source,
        field) -> new ParseIpLeadingZerosRejectedEvaluator.Factory(
            source,
            field,
            driverContext -> new BreakingBytesRefBuilder(driverContext.breaker(), "to_ip")
        );

    private static final BuildFactory FROM_KEYWORD_LEADING_ZEROS_ARE_DECIMAL = (
        source,
        field) -> new ParseIpLeadingZerosAreDecimalEvaluator.Factory(
        source,
        field,
        driverContext -> new BreakingBytesRefBuilder(driverContext.breaker(), "to_ip")
    );

    private static final BuildFactory FROM_KEYWORD_LEADING_ZEROS_ARE_OCTAL = (
        source,
        field) -> new ParseIpLeadingZerosAreOctalEvaluator.Factory(
        source,
        field,
        driverContext -> new BreakingBytesRefBuilder(driverContext.breaker(), "to_ip")
    );
}
