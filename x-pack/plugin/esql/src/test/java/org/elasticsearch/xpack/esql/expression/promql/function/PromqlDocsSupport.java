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
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

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

    /**
     * First line of every generated markdown snippet, matching the ES|QL generated-snippet convention so the files are
     * clearly marked as machine-generated and never hand-edited.
     */
    private static final String MD_WARNING = "% This is generated by ESQL's PromqlKibanaDefinitionGeneratorTests. "
        + "Do not edit it. See docs/reference/query-languages/esql/README.md for how to regenerate it.";

    /**
     * Inline preview marker emitted on every function snippet. Kept consistent with the {@code promql.md} page
     * frontmatter ({@code stack: preview 9.4.0}); update here if the PromQL function feature availability changes.
     */
    private static final String MD_APPLIES_TO = "{applies_to}`stack: preview 9.4.0` {applies_to}`serverless: preview`";

    private static final String RATE_FAMILY_NOTE =
        "Requires a counter input; non-counter inputs are automatically coerced with `to_counter`. The metric's "
            + "configured temporality (cumulative or delta) is honored. Native histogram inputs are not supported, and "
            + "the result is always a `double`.";

    private static final String GAUGE_FAMILY_NOTE =
        "This is a gauge-only function: counter inputs are automatically converted to a gauge with `to_gauge`. Native "
            + "histogram inputs are not supported.";

    private static final String DOMAIN_PLUS_MINUS_ONE_NOTE =
        "For inputs outside the range [-1, 1], {{es}} returns `null` and emits a warning, rather than the `NaN` that "
            + "Prometheus returns.";

    private static final String OVERFLOW_NOTE =
        "On numeric overflow for large-magnitude inputs, {{es}} returns `null` and emits a warning, rather than the "
            + "`±Inf` that Prometheus returns.";

    private static final String WHOLE_NUMBER_TYPE_NOTE =
        "Preserves the input's integer or floating-point type: whole-number inputs are returned unchanged instead of "
            + "being converted to a float.";

    private static final String QUANTILE_NOTE =
        "Computed using the {{es}} t-digest percentile aggregation, so results are approximate and may differ slightly "
            + "from Prometheus's exact linear interpolation, particularly for small sample sets.";

    /**
     * Per-function "Differences from Prometheus" notes, keyed by function name. Only functions whose {{es}} behavior
     * diverges from the Prometheus reference have an entry; the note is appended to the generated snippet. Every entry
     * is grounded in the implementation: see the cited source for each claim.
     */
    private static final Map<String, String> DIFFERENCES = Map.ofEntries(
        // counterSupport=REQUIRED + InjectTemporality; no native histogram support.
        Map.entry("rate", RATE_FAMILY_NOTE),
        Map.entry("increase", RATE_FAMILY_NOTE),
        Map.entry("irate", RATE_FAMILY_NOTE),
        // counterSupport=UNSUPPORTED: counters wrapped with ToGauge in PromqlFunctionCall.
        Map.entry("delta", GAUGE_FAMILY_NOTE),
        Map.entry("idelta", GAUGE_FAMILY_NOTE),
        Map.entry("deriv", GAUGE_FAMILY_NOTE),
        // Count/CountOverTime return a long count.
        Map.entry("count", "Returns a `long` integer count rather than a floating-point value."),
        Map.entry("count_over_time", "Returns a `long` integer count rather than a floating-point value."),
        // PresentOverTime/AbsentOverTime return boolean (returnType = { "boolean" }), not numeric 1.
        Map.entry(
            "present_over_time",
            "Returns a `boolean` (`true` when the range vector has at least one sample) rather than the numeric value "
                + "`1` that Prometheus returns."
        ),
        Map.entry(
            "absent_over_time",
            "Returns a `boolean` (`true` when the range vector has no samples) rather than the numeric value `1` that "
                + "Prometheus returns."
        ),
        // ES|QL math evaluators return null + warning on domain errors instead of NaN/±Inf.
        Map.entry(
            "ln",
            "For an input of zero or a negative number, {{es}} returns `null` and emits a warning, rather than the "
                + "`-Inf` (for zero) or `NaN` (for negatives) that Prometheus returns."
        ),
        Map.entry(
            "log2",
            "For an input of zero or a negative number, {{es}} returns `null` and emits a warning, rather than the "
                + "`-Inf` (for zero) or `NaN` (for negatives) that Prometheus returns."
        ),
        Map.entry(
            "log10",
            "For an input of zero or a negative number, {{es}} returns `null` and emits a warning, rather than the "
                + "`-Inf` (for zero) or `NaN` (for negatives) that Prometheus returns."
        ),
        Map.entry(
            "sqrt",
            "For a negative input, {{es}} returns `null` and emits a warning, rather than the `NaN` that Prometheus " + "returns."
        ),
        Map.entry("asin", DOMAIN_PLUS_MINUS_ONE_NOTE),
        Map.entry("acos", DOMAIN_PLUS_MINUS_ONE_NOTE),
        Map.entry(
            "acosh",
            "For inputs below 1, {{es}} returns `null` and emits a warning, rather than the `NaN` that Prometheus " + "returns."
        ),
        Map.entry(
            "atanh",
            "For an input whose absolute value is 1 or greater, {{es}} returns `null` and emits a warning, rather than "
                + "the `±Inf` or `NaN` that Prometheus returns."
        ),
        Map.entry("sinh", OVERFLOW_NOTE),
        Map.entry("cosh", OVERFLOW_NOTE),
        // abs/ceil/floor preserve integer types; abs of the minimum integer/long value overflows.
        Map.entry(
            "abs",
            "Preserves the input's integer or floating-point type instead of always returning a float. For the minimum "
                + "`integer` or `long` value, whose absolute value cannot be represented, {{es}} returns `null` and "
                + "emits a warning."
        ),
        Map.entry("ceil", WHOLE_NUMBER_TYPE_NOTE),
        Map.entry("floor", WHOLE_NUMBER_TYPE_NOTE),
        // clamp is a surrogate for clamp_max(clamp_min(...)), so it lacks the min > max empty-vector special case.
        Map.entry(
            "clamp",
            "Does not implement Prometheus's special case of returning an empty vector when `min` is greater than "
                + "`max`; it always returns clamped values."
        ),
        // Percentile/PercentileOverTime use t-digest; the φ argument is scaled to the 0..100 percentile range.
        Map.entry("quantile", QUANTILE_NOTE),
        Map.entry("quantile_over_time", QUANTILE_NOTE),
        // PromqlHistogramQuantile is classic-histogram only (le buckets); native histograms are not supported.
        Map.entry(
            "histogram_quantile",
            "Only classic histograms, represented by cumulative `le` bucket series, are supported. Prometheus native "
                + "histograms are not supported."
        )
    );

    private PromqlDocsSupport() {}

    public static void entrypoint(DocsV3Support.Callbacks callbacks) throws Exception {
        var functions = PromqlFunctionRegistry.INSTANCE.allFunctions();
        Map<DocCategory, SortedSet<String>> byCategory = new EnumMap<>(DocCategory.class);
        for (var def : functions) {
            genFunctionDocs(def, callbacks);
            genFunctionMarkdown(def, callbacks);
            byCategory.computeIfAbsent(categoryOf(def), k -> new TreeSet<>()).add(def.name());
        }
        for (var opDef : OPERATOR_DEFS) {
            genOperatorDocs(opDef, callbacks);
        }
        genCategoryLists(functions.size(), byCategory, callbacks);
        genNotSupported(callbacks);
    }

    private static void genFunctionDocs(PromqlFunctionDefinition def, DocsV3Support.Callbacks callbacks) throws Exception {
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

    /**
     * Renders the self-contained markdown reference snippet for a single function and writes it to the snippets temp
     * tree. The file is byte-for-byte asserted in CI, so the layout here is the source of truth for the committed
     * snippet under {@code docs/.../promql/_snippets/generated/x-pack-esql/functions/<name>.md}.
     */
    private static void genFunctionMarkdown(PromqlFunctionDefinition def, DocsV3Support.Callbacks callbacks) throws Exception {
        List<String> blocks = new ArrayList<>();
        blocks.add(MD_WARNING);
        blocks.add("## `" + def.name() + "` [promql-fn-" + def.name() + "]");
        blocks.add(MD_APPLIES_TO);
        blocks.add(def.description());
        blocks.add("Returns `" + mapDataType(def.functionType().outputType()) + "`.");

        if (def.params().isEmpty() == false) {
            StringBuilder params = new StringBuilder("### Parameters\n");
            for (var p : def.params()) {
                params.append("\n`").append(p.name()).append("` (`").append(mapDataType(p.type())).append("`");
                if (p.optional()) {
                    params.append(", optional");
                }
                params.append(")\n:   ").append(p.description()).append("\n");
            }
            blocks.add(params.toString().stripTrailing());
        }

        if (def.examples().isEmpty() == false) {
            // docs-builder has no `promql` highlighter (see elastic/docs-builder hljs.ts), so a bare fence avoids
            // "Unknown language" warnings. These examples are bare PromQL expressions, not ES|QL.
            blocks.add("### Example\n\n```\n" + String.join("\n", def.examples()) + "\n```");
        }

        String note = DIFFERENCES.get(def.name());
        if (note != null) {
            blocks.add("### Differences from Prometheus\n\n" + note);
        }

        String rendered = String.join("\n\n", blocks) + "\n";
        callbacks.write(snippetsFunctionsDir(), def.name(), "md", rendered, false);
    }

    /**
     * Emits one generated list snippet per documentation category. Each snippet includes its functions' snippets in
     * alphabetical order; the category page includes only the matching list snippet, so page membership and ordering
     * are generated from the registry and asserted in CI. Fails fast if categorization does not cover every function
     * exactly once.
     */
    private static void genCategoryLists(
        int functionCount,
        Map<DocCategory, SortedSet<String>> byCategory,
        DocsV3Support.Callbacks callbacks
    ) throws Exception {
        int categorized = byCategory.values().stream().mapToInt(SortedSet::size).sum();
        if (categorized != functionCount) {
            throw new IllegalStateException(
                "PromQL docs categorization mismatch: " + categorized + " categorized vs " + functionCount + " registered functions"
            );
        }
        Path dir = snippetsFunctionsDir().resolve("lists");
        for (DocCategory category : DocCategory.values()) {
            SortedSet<String> names = byCategory.getOrDefault(category, new TreeSet<>());
            List<String> blocks = new ArrayList<>();
            blocks.add(MD_WARNING);
            for (String name : names) {
                blocks.add(":::{include} ../" + name + ".md\n:::");
            }
            callbacks.write(dir, category.slug, "md", String.join("\n\n", blocks) + "\n", false);
        }
    }

    /**
     * Emits the generated "Not yet supported" snippet (body only, no heading) listing the PromQL functions that are
     * recognized but not yet implemented, straight from the registry.
     */
    private static void genNotSupported(DocsV3Support.Callbacks callbacks) throws Exception {
        StringBuilder body = new StringBuilder();
        for (String name : PromqlFunctionRegistry.INSTANCE.notImplementedFunctions()) {
            body.append("* `").append(name).append("`\n");
        }
        String rendered = MD_WARNING + "\n\n" + body.toString().stripTrailing() + "\n";
        callbacks.write(snippetsFunctionsDir(), "not-supported", "md", rendered, false);
    }

    private static Path snippetsFunctionsDir() {
        return PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("promql").resolve("_snippets").resolve("functions");
    }

    /**
     * Resolves the documentation category for a function. Defaults from {@link FunctionType}, with name overrides for
     * the two {@code SCALAR}-typed functions ({@code pi}, {@code time}) that belong with math / date-time. Throws on
     * any function that does not map to a category, forcing a deliberate decision when a new function is added.
     */
    private static DocCategory categoryOf(PromqlFunctionDefinition def) {
        if (def.name().equals("pi")) {
            return DocCategory.MATH;
        }
        if (def.name().equals("time")) {
            return DocCategory.DATE_TIME;
        }
        return switch (def.functionType()) {
            case WITHIN_SERIES_AGGREGATION -> DocCategory.RANGE_VECTOR;
            case ACROSS_SERIES_AGGREGATION -> DocCategory.AGGREGATION;
            case HISTOGRAM -> DocCategory.HISTOGRAM;
            case VALUE_TRANSFORMATION -> DocCategory.MATH;
            case TIME_EXTRACTION -> DocCategory.DATE_TIME;
            case VECTOR_CONVERSION, SCALAR_CONVERSION -> DocCategory.CONVERSION;
            case SCALAR, METADATA_MANIPULATION -> throw new IllegalStateException(
                "PromQL function ["
                    + def.name()
                    + "] has FunctionType ["
                    + def.functionType()
                    + "] with no docs category; add a name override in categoryOf"
            );
        };
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

    /**
     * Documentation categories for grouping PromQL functions into reference pages. The declaration order is the order
     * categories appear in the docs; {@code slug} is both the list-snippet filename and the category page filename.
     */
    enum DocCategory {
        RANGE_VECTOR("range-vector"),
        AGGREGATION("aggregation"),
        HISTOGRAM("histogram"),
        MATH("math"),
        DATE_TIME("date-time"),
        CONVERSION("conversion");

        final String slug;

        DocCategory(String slug) {
            this.slug = slug;
        }
    }

    record ParamDef(String name, String type, boolean optional, String description) {}

    record SignatureDef(List<ParamDef> params, boolean variadic, String returnType) {}

    record OperatorDefinition(String name, String operator, OperatorKind kind, String description, String example) {}
}
