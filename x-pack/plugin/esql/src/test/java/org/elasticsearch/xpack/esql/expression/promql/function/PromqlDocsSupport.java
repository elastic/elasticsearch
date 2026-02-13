/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.promql.function;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.expression.function.DocsV3Support;
import org.elasticsearch.xpack.esql.plan.logical.promql.PromqlDataType;

import java.nio.file.Path;
import java.util.List;

/**
 * Generates PromQL Kibana definition JSON files.
 */
public final class PromqlDocsSupport {

    static final List<OperatorDefinition> OPERATOR_DEFS = List.of(
        // Arithmetic binary operators
        new OperatorDefinition(
            "add",
            "+",
            OperatorKind.BINARY,
            "Adds the two operands element-wise. For vector–vector operations, the metric name is dropped.",
            "requests_total + requests_errors"
        ),
        new OperatorDefinition(
            "sub",
            "-",
            OperatorKind.BINARY,
            "Subtracts the right operand from the left operand element-wise. For vector–vector operations, the metric name is dropped.",
            "requests_total - requests_errors"
        ),
        new OperatorDefinition(
            "mul",
            "*",
            OperatorKind.BINARY,
            "Multiplies the two operands element-wise. For vector–vector operations, the metric name is dropped.",
            "100 * rate(requests_total[5m])"
        ),
        new OperatorDefinition(
            "div",
            "/",
            OperatorKind.BINARY,
            "Divides the left operand by the right operand element-wise. For vector–vector operations, the metric name is dropped.",
            "requests_errors / requests_total"
        ),
        new OperatorDefinition(
            "mod",
            "%",
            OperatorKind.BINARY,
            "Returns the remainder of dividing the left operand by the right operand element-wise. "
                + "For vector\u2013vector operations, the metric name is dropped.",
            "metric % 10"
        ),
        new OperatorDefinition(
            "pow",
            "^",
            OperatorKind.BINARY,
            "Raises the left operand to the power of the right operand element-wise. "
                + "For vector\u2013vector operations, the metric name is dropped.",
            "metric ^ 2"
        ),

        // Comparison binary operators
        new OperatorDefinition(
            "eq",
            "==",
            OperatorKind.BINARY,
            "Compares elements for equality. By default, acts as a filter. With the bool modifier, returns 0 or 1.",
            "requests_total == 256"
        ),
        new OperatorDefinition(
            "neq",
            "!=",
            OperatorKind.BINARY,
            "Compares elements for inequality. By default, acts as a filter. With the bool modifier, returns 0 or 1.",
            "requests_error != 0"
        ),
        new OperatorDefinition(
            "gt",
            ">",
            OperatorKind.BINARY,
            "Compares elements where the left operand is greater than the right. "
                + "By default, acts as a filter. With the bool modifier, returns 0 or 1.",
            "requests_total > 256"
        ),
        new OperatorDefinition(
            "gte",
            ">=",
            OperatorKind.BINARY,
            "Compares elements where the left operand is greater than or equal to the right. "
                + "By default, acts as a filter. With the bool modifier, returns 0 or 1.",
            "requests_total >= 256"
        ),
        new OperatorDefinition(
            "lt",
            "<",
            OperatorKind.BINARY,
            "Compares elements where the left operand is less than the right. "
                + "By default, acts as a filter. With the bool modifier, returns 0 or 1.",
            "requests_total < 256"
        ),
        new OperatorDefinition(
            "lte",
            "<=",
            OperatorKind.BINARY,
            "Compares elements where the left operand is less than or equal to the right. "
                + "By default, acts as a filter. With the bool modifier, returns 0 or 1.",
            "requests_total <= 256"
        ),

        // Set binary operators
        new OperatorDefinition(
            "and",
            "and",
            OperatorKind.SET_BINARY,
            "Returns elements from the left vector that have matching label sets in the right vector. "
                + "Values and metric name come from the left-hand side.",
            "vector1 and vector2"
        ),
        new OperatorDefinition(
            "or",
            "or",
            OperatorKind.SET_BINARY,
            "Returns all elements from the left vector plus elements from the right vector that have no matching label sets in the left.",
            "vector1 or vector2"
        ),
        new OperatorDefinition(
            "unless",
            "unless",
            OperatorKind.SET_BINARY,
            "Returns elements from the left vector that have no matching label sets in the right vector.",
            "vector1 unless vector2"
        ),

        // Unary operator
        new OperatorDefinition(
            "neg",
            "-",
            OperatorKind.UNARY,
            "Negates the instant vector or scalar by inverting the sign of each sample value.",
            "-requests_total"
        ),

        // Label matching operators
        new OperatorDefinition(
            "label_eq",
            "=",
            OperatorKind.LABEL_MATCHING,
            "Matches labels exactly equal to the provided string.",
            "requests_total{method=\"GET\"}"
        ),
        new OperatorDefinition(
            "label_neq",
            "!=",
            OperatorKind.LABEL_MATCHING,
            "Matches labels not equal to the provided string.",
            "requests_total{method!=\"DELETE\"}"
        ),
        new OperatorDefinition(
            "label_re",
            "=~",
            OperatorKind.LABEL_MATCHING,
            "Matches labels whose value satisfies the provided regular expression.",
            "requests_total{method=~\"GET|POST\"}"
        ),
        new OperatorDefinition(
            "label_nre",
            "!~",
            OperatorKind.LABEL_MATCHING,
            "Matches labels whose value does not satisfy the provided regular expression.",
            "requests_total{method!~\"DELETE|PUT\"}"
        )
    );
    private static final Logger logger = LogManager.getLogger(PromqlDocsSupport.class);
    private static final String SPEC_SITE = "https://prometheus.io/docs/prometheus/latest/querying";
    private static final String COMMENT_HEADER = "PromQL function definition for Kibana";
    private static final String COMMENT_FUNCTION = COMMENT_HEADER + ". See " + SPEC_SITE + "/functions/";
    private static final String COMMENT_OPERATOR = COMMENT_HEADER + ". See " + SPEC_SITE + "/operators/";

    private PromqlDocsSupport() {}

    public static void entrypoint(DocsV3Support.Callbacks callbacks) throws Exception {
        for (var def : PromqlFunctionRegistry.INSTANCE.allFunctions()) {
            genFunctionDocs(def, callbacks);
        }
        for (var opDef : OPERATOR_DEFS) {
            genOperatorDocs(opDef, callbacks);
        }
    }

    private static void genFunctionDocs(PromqlFunctionRegistry.FunctionDefinition def, DocsV3Support.Callbacks callbacks) throws Exception {
        List<ParamDef> params = def.params()
            .stream()
            .map(p -> new ParamDef(p.name(), mapDataType(p.type()), p.optional(), p.description()))
            .toList();

        List<SignatureDef> signatures = List.of(new SignatureDef(params, false, mapDataType(def.functionType().outputType())));

        genDocs(
            "definition/functions",
            COMMENT_FUNCTION,
            mapFunctionType(def.functionType()),
            null,
            def.name(),
            def.description(),
            signatures,
            def.examples(),
            callbacks
        );
    }

    private static void genOperatorDocs(OperatorDefinition opDef, DocsV3Support.Callbacks callbacks) throws Exception {
        genDocs(
            "definition/operators",
            COMMENT_OPERATOR,
            opDef.kind().typeName,
            opDef.operator(),
            opDef.name(),
            opDef.description(),
            opDef.kind().signatures(),
            List.of(opDef.example()),
            callbacks
        );
    }

    private static void genDocs(
        String subdir,
        String comment,
        String type,
        String operator,
        String name,
        String description,
        List<SignatureDef> signatures,
        List<String> examples,
        DocsV3Support.Callbacks callbacks
    ) throws Exception {
        try (XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().lfAtEnd()) {
            builder.startObject();
            builder.field("comment", comment);
            builder.field("type", type);
            if (operator != null) {
                builder.field("operator", operator);
            }
            builder.field("name", name);
            builder.field("description", description);

            builder.startArray("signatures");
            for (SignatureDef sig : signatures) {
                builder.startObject();
                builder.startArray("params");
                for (ParamDef param : sig.params()) {
                    builder.startObject();
                    builder.field("name", param.name());
                    builder.field("type", param.type());
                    builder.field("optional", param.optional());
                    builder.field("description", param.description());
                    builder.endObject();
                }
                builder.endArray();
                builder.field("variadic", sig.variadic());
                builder.field("returnType", sig.returnType());
                builder.endObject();
            }
            builder.endArray();

            builder.array("examples", examples.toArray(String[]::new));
            builder.field("preview", true);
            builder.field("snapshot_only", false);
            builder.endObject();

            String rendered = Strings.toString(builder);
            logger.info("Writing PromQL kibana definition for [{}]", name);
            Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("promql").resolve("kibana").resolve(subdir);
            callbacks.write(dir, name, "json", rendered, false);
        }
    }

    private static String mapFunctionType(FunctionType type) {
        return switch (type) {
            case WITHIN_SERIES_AGGREGATION -> "within_series";
            case ACROSS_SERIES_AGGREGATION -> "across_series";
            case SCALAR -> "scalar";
            case METADATA_MANIPULATION -> "metadata";
            case TIME_EXTRACTION -> "time";
            case HISTOGRAM -> "histogram";
            case VALUE_TRANSFORMATION -> "value_transformation";
            case VECTOR_CONVERSION -> "vector_conversion";
            case SCALAR_CONVERSION -> "scalar_conversion";
        };
    }

    private static String mapDataType(PromqlDataType type) {
        return type.toString();
    }

    enum OperatorKind {
        BINARY("operator") {
            @Override
            List<SignatureDef> signatures() {
                return List.of(
                    binarySig(V, "Instant vector.", V, "Instant vector.", V),
                    binarySig(V, "Instant vector.", S, "Scalar.", V),
                    binarySig(S, "Scalar.", V, "Instant vector.", V),
                    binarySig(S, "Scalar.", S, "Scalar.", S)
                );
            }
        },
        SET_BINARY("operator") {
            @Override
            List<SignatureDef> signatures() {
                return List.of(binarySig(V, "Instant vector.", V, "Instant vector.", V));
            }
        },
        UNARY("operator") {
            @Override
            List<SignatureDef> signatures() {
                return List.of(
                    new SignatureDef(List.of(new ParamDef("field", V, false, "Instant vector.")), false, V),
                    new SignatureDef(List.of(new ParamDef("field", S, false, "Scalar.")), false, S)
                );
            }
        },
        LABEL_MATCHING("label_matching_operator") {
            @Override
            List<SignatureDef> signatures() {
                return List.of(
                    new SignatureDef(
                        List.of(
                            new ParamDef("label", "string", false, "Label name."),
                            new ParamDef("value", "string", false, "String or regex.")
                        ),
                        false,
                        V
                    )
                );
            }
        };

        private static final String V = mapDataType(PromqlDataType.INSTANT_VECTOR);
        private static final String S = mapDataType(PromqlDataType.SCALAR);

        final String typeName;

        OperatorKind(String typeName) {
            this.typeName = typeName;
        }

        private static SignatureDef binarySig(String lhsType, String lhsDesc, String rhsType, String rhsDesc, String returnType) {
            return new SignatureDef(
                List.of(new ParamDef("lhs", lhsType, false, lhsDesc), new ParamDef("rhs", rhsType, false, rhsDesc)),
                false,
                returnType
            );
        }

        abstract List<SignatureDef> signatures();
    }

    record ParamDef(String name, String type, boolean optional, String description) {}

    record SignatureDef(List<ParamDef> params, boolean variadic, String returnType) {}

    record OperatorDefinition(String name, String operator, OperatorKind kind, String description, String example) {}
}
