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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Generates the PromQL reference documentation from the function registry: the Kibana definition JSON files, the
 * per-function and per-category markdown snippets, and the "Not yet supported" list. All outputs are asserted
 * byte-for-byte in CI, so this class is the source of truth for the committed files under
 * {@code docs/reference/query-languages/promql/}.
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

    /**
     * Docs-root path of the PromQL reference pages. Cross-page links are built as absolute paths from this root,
     * matching the docs framework convention (see {@code DocsV3Support#makeLink}) so they stay valid regardless of
     * where the including snippet lives.
     */
    private static final String DOCS_ROOT = "/reference/query-languages/promql";
    private static final String COMMENT_HEADER = "PromQL function definition for Kibana";
    private static final String COMMENT_FUNCTION = COMMENT_HEADER + ". See " + SPEC_SITE + "/functions/";
    private static final String COMMENT_OPERATOR = COMMENT_HEADER + ". See " + SPEC_SITE + "/operators/";

    /**
     * First line of every generated markdown snippet, matching the ES|QL generated-snippet convention so the files are
     * clearly marked as machine-generated and never hand-edited.
     */
    private static final String MD_WARNING = "% This is generated by ESQL's PromqlKibanaDefinitionGeneratorTests. "
        + "Do not edit it. See docs/reference/query-languages/esql/README.md for how to regenerate it.";

    private PromqlDocsSupport() {}

    public static void entrypoint(DocsV3Support.Callbacks callbacks) throws Exception {
        var functions = PromqlFunctionRegistry.INSTANCE.allFunctions();
        Map<FunctionDocCategory, SortedSet<String>> byCategory = new EnumMap<>(FunctionDocCategory.class);
        Map<String, PromqlFunctionDefinition> byName = new HashMap<>();
        for (var def : functions) {
            genFunctionDocs(def, callbacks);
            genFunctionMarkdown(def, callbacks);
            genBriefSummary(def, callbacks);
            byCategory.computeIfAbsent(categoryOf(def), k -> new TreeSet<>()).add(def.name());
            byName.put(def.name(), def);
        }
        for (var opDef : OPERATOR_DEFS) {
            genOperatorDocs(opDef, callbacks);
        }
        genCategoryLists(functions.size(), byCategory, callbacks);
        genFunctionOverviewLists(byCategory, byName, callbacks);
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
            fullDescription(def),
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
        blocks.add("## `" + def.name() + "` [" + functionAnchor(def.name()) + "]");
        blocks.add(appliesToBadge(def));
        blocks.add(include("brief-summary/" + def.name() + ".md"));
        if (def.extendedDescription() != null) {
            blocks.add(def.extendedDescription());
        }
        blocks.add("**Return type**\n\n`" + mapDataType(def.functionType().outputType()) + "`");

        if (def.params().isEmpty() == false) {
            StringBuilder params = new StringBuilder("**Parameters**\n");
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
            blocks.add("**Example**\n\n```\n" + String.join("\n", def.examples()) + "\n```");
        }

        String note = def.differenceFromPrometheus();
        if (note != null) {
            blocks.add("**Differences from Prometheus**\n\n" + note);
        }

        String rendered = String.join("\n\n", blocks) + "\n";
        callbacks.write(snippetsFunctionsDir(), def.name(), "md", rendered, false);
    }

    /**
     * Builds the per-function {@code applies_to} badge from the function's declared
     * {@link PromqlFunctionDefinition#stack() stack availability}. Serverless availability is omitted on purpose:
     * implemented PromQL functions are generally available in serverless, which is declared once in the docs page
     * frontmatter rather than repeated on every function. Fails fast if a registered function has not declared its
     * availability, so adding a function to the registry without a {@code stack(...)} entry breaks doc generation.
     */
    private static String appliesToBadge(PromqlFunctionDefinition def) {
        if (def.stack().isEmpty()) {
            throw new IllegalStateException(
                "PromQL function ["
                    + def.name()
                    + "] is registered but declares no stack availability for docs; add a stack(...) entry to its PROMQL_DEFINITION"
            );
        }
        String stack = def.stack().stream().map(PromqlFunctionDefinition.StackAvailability::appliesTo).collect(Collectors.joining(", "));
        return "{applies_to}`stack: " + stack + "`";
    }

    /**
     * Emits one generated list snippet per documentation category. Each snippet includes its functions' snippets in
     * alphabetical order; the category page includes only the matching list snippet, so page membership and ordering
     * are generated from the registry and asserted in CI. Fails fast if categorization does not cover every function
     * exactly once.
     */
    private static void genCategoryLists(
        int functionCount,
        Map<FunctionDocCategory, SortedSet<String>> byCategory,
        DocsV3Support.Callbacks callbacks
    ) throws Exception {
        int categorized = byCategory.values().stream().mapToInt(SortedSet::size).sum();
        if (categorized != functionCount) {
            throw new IllegalStateException(
                "PromQL docs categorization mismatch: " + categorized + " categorized vs " + functionCount + " registered functions"
            );
        }
        Path dir = snippetsFunctionsDir().resolve("lists");
        for (FunctionDocCategory category : FunctionDocCategory.values()) {
            SortedSet<String> names = byCategory.getOrDefault(category, new TreeSet<>());
            List<String> blocks = new ArrayList<>();
            blocks.add(MD_WARNING);
            for (String name : names) {
                blocks.add(include("../" + name + ".md"));
            }
            callbacks.write(dir, category.slug, "md", String.join("\n\n", blocks) + "\n", false);
        }
    }

    /**
     * Emits the generated one-line brief-summary snippet for a single function (mirroring the ES|QL brief-summary
     * snippets, but using a lowercase {@code brief-summary/} directory because docs-builder rejects path segments with
     * uppercase letters). This is the single materialization of the function's description string: it is included both
     * by the function's own page snippet ({@link #genFunctionMarkdown}) and by the per-category overview lists
     * ({@link #genFunctionOverviewLists}), so the description is never duplicated across generated files.
     */
    private static void genBriefSummary(PromqlFunctionDefinition def, DocsV3Support.Callbacks callbacks) throws Exception {
        String summary = def.description().replaceAll("\\s+", " ").strip();
        String rendered = MD_WARNING + "\n\n" + summary + "\n";
        callbacks.write(snippetsFunctionsDir().resolve("brief-summary"), def.name(), "md", rendered, false);
    }

    /**
     * Emits one generated overview-list snippet per documentation category, matching the ES|QL "Functions overview"
     * layout: each entry is a function link to its section on the category page, the {@code applies_to}
     * availability badge on the same line, and the function's brief summary included as a block on the next line.
     * These snippets back the searchable "Functions overview" section of {@code functions.md} and are asserted
     * byte-for-byte in CI.
     */
    private static void genFunctionOverviewLists(
        Map<FunctionDocCategory, SortedSet<String>> byCategory,
        Map<String, PromqlFunctionDefinition> byName,
        DocsV3Support.Callbacks callbacks
    ) throws Exception {
        Path dir = snippetsFunctionsDir().resolve("lists");
        for (FunctionDocCategory category : FunctionDocCategory.values()) {
            SortedSet<String> names = byCategory.getOrDefault(category, new TreeSet<>());
            List<String> blocks = new ArrayList<>();
            blocks.add(MD_WARNING);
            StringBuilder list = new StringBuilder();
            for (String name : names) {
                list.append("* [`")
                    .append(name)
                    .append("`](")
                    .append(functionPageLink(category, name))
                    .append(") ")
                    .append(appliesToBadge(byName.get(name)))
                    .append("\n  :::{include} ../brief-summary/")
                    .append(name)
                    .append(".md\n  :::\n");
            }
            if (list.isEmpty() == false) {
                blocks.add(list.toString().stripTrailing());
            }
            callbacks.write(dir, category.slug + "-overview", "md", String.join("\n\n", blocks) + "\n", false);
        }
    }

    /**
     * The function's full description for the Kibana definition JSON: the brief summary plus the optional extended
     * detail, matching what the dedicated function page renders. (The functions overview deliberately uses only the
     * brief {@link PromqlFunctionDefinition#description()}.)
     */
    private static String fullDescription(PromqlFunctionDefinition def) {
        if (def.extendedDescription() == null) {
            return def.description();
        }
        return def.description() + " " + def.extendedDescription();
    }

    /** Renders a docs-builder include directive for a snippet at the given path, relative to the including file. */
    private static String include(String relativePath) {
        return ":::{include} " + relativePath + "\n:::";
    }

    /** The stable docs anchor for a function's section heading, for example {@code promql-fn-rate}. */
    private static String functionAnchor(String name) {
        return "promql-fn-" + name;
    }

    /**
     * Absolute docs link to a function's section on its category page, for example
     * {@code /reference/query-languages/promql/functions/math.md#promql-fn-abs}.
     */
    private static String functionPageLink(FunctionDocCategory category, String name) {
        return DOCS_ROOT + "/functions/" + category.slug + ".md#" + functionAnchor(name);
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
    private static FunctionDocCategory categoryOf(PromqlFunctionDefinition def) {
        if (def.name().equals("pi")) {
            return FunctionDocCategory.MATH;
        }
        if (def.name().equals("time")) {
            return FunctionDocCategory.DATE_TIME;
        }
        return switch (def.functionType()) {
            case WITHIN_SERIES_AGGREGATION -> FunctionDocCategory.RANGE_VECTOR;
            case ACROSS_SERIES_AGGREGATION -> FunctionDocCategory.AGGREGATION;
            case HISTOGRAM -> FunctionDocCategory.HISTOGRAM;
            case VALUE_TRANSFORMATION -> FunctionDocCategory.MATH;
            case TIME_EXTRACTION -> FunctionDocCategory.DATE_TIME;
            case VECTOR_CONVERSION, SCALAR_CONVERSION -> FunctionDocCategory.CONVERSION;
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
    enum FunctionDocCategory {
        RANGE_VECTOR("range-vector"),
        AGGREGATION("aggregation"),
        HISTOGRAM("histogram"),
        MATH("math"),
        DATE_TIME("date-time"),
        CONVERSION("conversion");

        final String slug;

        FunctionDocCategory(String slug) {
            this.slug = slug;
        }
    }

    record ParamDef(String name, String type, boolean optional, String description) {}

    record SignatureDef(List<ParamDef> params, boolean variadic, String returnType) {}

    record OperatorDefinition(String name, String operator, OperatorKind kind, String description, String example) {}
}
