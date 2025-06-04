/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import com.unboundid.util.NotNull;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.fulltext.MatchOperator;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.RLike;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.WildcardLike;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNotNull;
import org.elasticsearch.xpack.esql.expression.predicate.nulls.IsNull;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Add;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Div;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mod;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Mul;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Neg;
import org.elasticsearch.xpack.esql.expression.predicate.operator.arithmetic.Sub;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.GreaterThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThan;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.LessThanOrEqual;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.constructorWithFunctionInfo;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.definition;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.functionRegistered;
import static org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase.shouldHideSignature;
import static org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry.mapParam;
import static org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry.param;
import static org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry.paramWithoutAnnotation;

/**
 * This class exists to support the new Docs V3 system.
 * Between Elasticsearch 8.x and 9.0 the reference documents were completely re-written, with several key changes:
 * <ol>
 *     <li>Port from ASCIIDOC to MD (markdown)</li>
 *     <li>Restructures all Elastic docs with clearer separate between reference docs and other docs</li>
 *     <li>All versions published from the main branch,
 *     requiring version specific information to be included in the docs as appropriate</li>
 *     <li>Including sub-docs inside bigger docs works differently, requiring a new directory structure</li>
 *     <li>Images and Kibana docs cannot be in the same location as snippets</li>
 * </ol>
 *
 * For these reasons the docs generating code that used to live inside <code>AbstractFunctionTestCase</code> has been pulled out
 * and partially re-written to satisfy the above requirements.
 */
public abstract class DocsV3Support {

    protected static final String DOCS_WARNING =
        "% This is generated by ESQL's AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it.\n\n";

    static FunctionDocsSupport forFunctions(String name, Class<?> testClass) {
        return new FunctionDocsSupport(name, testClass);
    }

    static OperatorsDocsSupport forOperators(String name, Class<?> testClass) {
        return new OperatorsDocsSupport(name, testClass);
    }

    static void renderDocs(String name, Class<?> testClass) throws Exception {
        if (OPERATORS.containsKey(name)) {
            var docs = DocsV3Support.forOperators(name, testClass);
            docs.renderSignature();
            docs.renderDocs();
        } else if (functionRegistered(name)) {
            var docs = DocsV3Support.forFunctions(name, testClass);
            docs.renderSignature();
            docs.renderDocs();
        } else {
            LogManager.getLogger(testClass).info("Skipping rendering docs because the function '" + name + "' isn't registered");
        }
    }

    public static void renderNegatedOperator(
        @NotNull Constructor<?> ctor,
        String name,
        Function<String, String> description,
        Class<?> testClass
    ) throws Exception {
        var docs = forOperators("not " + name.toLowerCase(Locale.ROOT), testClass);
        docs.renderDocsForNegatedOperators(ctor, description);
    }

    private static final Map<String, String> MACROS = new HashMap<>();
    private static final Map<String, String> knownFiles;
    private static final Map<String, String> knownCommands;
    private static final Map<String, String> knownOperators;
    private static final Map<String, String> knownMapping;

    static {
        MACROS.put("wikipedia", "https://en.wikipedia.org/wiki");
        MACROS.put("javadoc8", "https://docs.oracle.com/javase/8/docs/api");
        MACROS.put("javadoc14", "https://docs.oracle.com/en/java/javase/14/docs/api");
        MACROS.put("javadoc", "https://docs.oracle.com/en/java/javase/11/docs/api");
        // This static list of known root query-languages/-*.md files is used to simulate old simple ascii-doc links
        knownFiles = Map.ofEntries(
            entry("esql-commands", "esql/esql-commands.md"),
            entry("esql-enrich-data", "esql/esql-enrich-data.md"),
            entry("esql-examples", "esql/esql-examples.md"),
            entry("esql-functions-operators", "esql/esql-functions-operators.md"),
            entry("esql-agg-functions", "esql/functions-operators/aggregation-functions.md"),
            entry("esql-implicit-casting", "esql/esql-implicit-casting.md"),
            entry("esql-metadata-fields", "esql/esql-metadata-fields.md"),
            entry("esql-multivalued-fields", "esql/esql-multivalued-fields.md"),
            entry("esql-process-data-with-dissect-grok", "esql/esql-process-data-with-dissect-grok.md"),
            entry("esql-syntax", "esql/esql-syntax.md"),
            entry("esql-time-spans", "esql/esql-time-spans.md"),
            entry("esql-limitations", "esql/limitations.md"),
            entry("esql-function-named-params", "esql/esql-syntax.md"),
            entry("query-dsl-query-string-query", "query-dsl/query-dsl-query-string-query.md"),
            entry("regexp-syntax", "query-dsl/regexp-syntax.md")
        );
        // Static links to the commands file
        knownCommands = Map.ofEntries(entry("where", "where"), entry("stats-by", "stats"));
        // Static links to handwritten operators files
        knownOperators = Map.ofEntries(entry("cast-operator", "Cast (::)"));
        // Static links to mapping-reference
        knownMapping = Map.ofEntries(
            entry("text", "/reference/elasticsearch/mapping-reference/text.md"),
            entry("semantic-text", "/reference/elasticsearch/mapping-reference/semantic-text.md"),
            entry("mapping-index", "/reference/elasticsearch/mapping-reference/mapping-index.md"),
            entry("doc-values", "/reference/elasticsearch/mapping-reference/doc-values.md")
        );
    }
    /**
     * Operators are unregistered functions.
     */
    static final Map<String, OperatorConfig> OPERATORS = Map.ofEntries(
        // Binary
        operatorEntry("equals", "==", Equals.class, OperatorCategory.BINARY),
        operatorEntry("not_equals", "!=", NotEquals.class, OperatorCategory.BINARY),
        operatorEntry("greater_than", ">", GreaterThan.class, OperatorCategory.BINARY),
        operatorEntry("greater_than_or_equal", ">=", GreaterThanOrEqual.class, OperatorCategory.BINARY),
        operatorEntry("less_than", "<", LessThan.class, OperatorCategory.BINARY),
        operatorEntry("less_than_or_equal", "<=", LessThanOrEqual.class, OperatorCategory.BINARY),
        operatorEntry("add", "+", Add.class, OperatorCategory.BINARY),
        operatorEntry("sub", "-", Sub.class, OperatorCategory.BINARY, "subtract"),
        operatorEntry("mul", "*", Mul.class, OperatorCategory.BINARY, "multiply"),
        operatorEntry("div", "/", Div.class, OperatorCategory.BINARY, "divide"),
        operatorEntry("mod", "%", Mod.class, OperatorCategory.BINARY, "modulo"),
        // Unary
        operatorEntry("neg", "-", Neg.class, OperatorCategory.UNARY, "negate"),
        // Logical
        operatorEntry("and", "AND", And.class, OperatorCategory.LOGICAL),
        operatorEntry("or", "OR", Or.class, OperatorCategory.LOGICAL),
        operatorEntry("not", "NOT", Not.class, OperatorCategory.LOGICAL),
        // NULL and IS NOT NULL
        operatorEntry("is_null", "IS NULL", IsNull.class, OperatorCategory.NULL_PREDICATES),
        operatorEntry("is_not_null", "IS NOT NULL", IsNotNull.class, OperatorCategory.NULL_PREDICATES),
        // LIKE AND RLIKE
        // case "rlike", "like", "in", "not_rlike", "not_like", "not_in" -> true;
        operatorEntry("like", "LIKE", WildcardLike.class, OperatorCategory.LIKE_AND_RLIKE, true),
        operatorEntry("rlike", "RLIKE", RLike.class, OperatorCategory.LIKE_AND_RLIKE, true),
        // IN
        operatorEntry("in", "IN", In.class, OperatorCategory.IN, true),
        // Search
        operatorEntry("match_operator", ":", MatchOperator.class, OperatorCategory.SEARCH)
    );

    /** Each grouping represents a subsection in the docs. Currently, this is manually maintained, but could be partially automated */
    public enum OperatorCategory {
        BINARY,
        UNARY,
        LOGICAL,
        NULL_PREDICATES,
        CAST,
        IN,
        LIKE_AND_RLIKE,
        SEARCH
    }

    /** Since operators do not exist in the function registry, we need an equivalent registry here in the docs generating code */
    public record OperatorConfig(
        String name,
        String symbol,
        Class<?> clazz,
        OperatorCategory category,
        boolean variadic,
        String titleName
    ) {
        public OperatorConfig(String name, String symbol, Class<?> clazz, OperatorCategory category) {
            this(name, symbol, clazz, category, false, null);
        }
    }

    private static Map.Entry<String, OperatorConfig> operatorEntry(
        String name,
        String symbol,
        Class<?> clazz,
        OperatorCategory category,
        boolean variadic
    ) {
        return entry(name, new OperatorConfig(name, symbol, clazz, category, variadic, null));
    }

    private static Map.Entry<String, OperatorConfig> operatorEntry(
        String name,
        String symbol,
        Class<?> clazz,
        OperatorCategory category,
        String displayName
    ) {
        return entry(name, new OperatorConfig(name, symbol, clazz, category, false, displayName));
    }

    private static Map.Entry<String, OperatorConfig> operatorEntry(String name, String symbol, Class<?> clazz, OperatorCategory category) {
        return entry(name, new OperatorConfig(name, symbol, clazz, category));
    }

    @FunctionalInterface
    interface TempFileWriter {
        void writeToTempDir(Path dir, String extension, String str) throws IOException;
    }

    private class DocsFileWriter implements TempFileWriter {
        @Override
        public void writeToTempDir(Path dir, String extension, String str) throws IOException {
            Files.createDirectories(dir);
            Path file = dir.resolve(name + "." + extension);
            Files.writeString(file, str);
            logger.info("Wrote to file: {}", file);
        }
    }

    /**
     * This class is used to check if a license requirement method exists in the test class.
     * This is used to add license requirement information to the generated documentation.
     */
    public static class LicenseRequirementChecker {
        private Method staticMethod;
        private Function<List<DataType>, License.OperationMode> fallbackLambda;

        public LicenseRequirementChecker(Class<?> testClass) {
            try {
                staticMethod = testClass.getMethod("licenseRequirement", List.class);
                if (License.OperationMode.class.equals(staticMethod.getReturnType()) == false
                    || java.lang.reflect.Modifier.isStatic(staticMethod.getModifiers()) == false) {
                    staticMethod = null; // Reset if the method doesn't match the signature
                }
            } catch (NoSuchMethodException e) {
                staticMethod = null;
            }

            if (staticMethod == null) {
                fallbackLambda = fieldTypes -> License.OperationMode.BASIC;
            }
        }

        public License.OperationMode invoke(List<DataType> fieldTypes) throws Exception {
            if (staticMethod != null) {
                return (License.OperationMode) staticMethod.invoke(null, fieldTypes);
            } else {
                return fallbackLambda.apply(fieldTypes);
            }
        }
    }

    protected final String category;
    protected final String name;
    protected final FunctionDefinition definition;
    protected final Logger logger;
    private final Supplier<Map<List<DataType>, DataType>> signatures;
    private TempFileWriter tempFileWriter;
    private final LicenseRequirementChecker licenseChecker;

    protected DocsV3Support(String category, String name, Class<?> testClass, Supplier<Map<List<DataType>, DataType>> signatures) {
        this(category, name, null, testClass, signatures);
    }

    private DocsV3Support(
        String category,
        String name,
        FunctionDefinition definition,
        Class<?> testClass,
        Supplier<Map<List<DataType>, DataType>> signatures
    ) {
        this.category = category;
        this.name = name;
        this.definition = definition == null ? definition(name) : definition;
        this.logger = LogManager.getLogger(testClass);
        this.signatures = signatures;
        this.tempFileWriter = new DocsFileWriter();
        this.licenseChecker = new LicenseRequirementChecker(testClass);
    }

    /** Used in tests to capture output for asserting on the content */
    void setTempFileWriter(TempFileWriter tempFileWriter) {
        this.tempFileWriter = tempFileWriter;
    }

    String replaceLinks(String text) {
        return replaceAsciidocLinks(replaceMacros(text));
    }

    private String replaceAsciidocLinks(String text) {
        Pattern pattern = Pattern.compile("<<([^>]*)>>");
        Matcher matcher = pattern.matcher(text);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String match = matcher.group(1);
            matcher.appendReplacement(result, getLink(match));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private String replaceMacros(String text) {
        Pattern pattern = Pattern.compile("\\{([^}]+)}(/[^\\[]+)\\[([^]]+)]");

        Matcher matcher = pattern.matcher(text);
        StringBuilder result = new StringBuilder();
        while (matcher.find()) {
            String macro = matcher.group(1);
            String path = matcher.group(2);
            String display = matcher.group(3);
            matcher.appendReplacement(result, getMacroLink(macro, path, display));
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private String getMacroLink(String macro, String path, String display) {
        if (MACROS.containsKey(macro) == false) {
            throw new IllegalArgumentException("Unknown macro [" + macro + "]");
        }
        return String.format(Locale.ROOT, "[%s](%s%s)", display, MACROS.get(macro), path);
    }

    /**
     * The new link syntax is extremely verbose.
     * Rather than make a large number of messy changes to the java files, we simply re-write the existing links to the new syntax.
     */
    private String getLink(String key) {
        String[] parts = key.split(",\\s*");
        // Inject esql examples
        if (parts[0].equals("load-esql-example")) {
            try {
                Map<String, String> example = Arrays.stream(parts[1].trim().split("\\s+"))
                    .map(s -> s.split("\\s*=\\s*", 2))
                    .map(p -> entry(p[0], p[1]))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                return "```esql\n" + loadExample(example.get("file"), example.get("tag")) + "\n```\n";
            } catch (IOException e) {
                return e.getMessage();
            }
        }
        // Known root query-languages markdown files
        if (knownFiles.containsKey(parts[0])) {
            return makeLink(key, "", "/reference/query-languages/" + knownFiles.get(parts[0]));
        }
        // Old-style links within ES|QL reference
        if (key.startsWith("esql-")) {
            String cmd = parts[0].replace("esql-", "");
            String parentFile = parentFileFor(cmd);
            return makeLink(key, "esql-", parentFile);
        }
        // Old-style links to Query DSL pages
        if (key.startsWith("query-dsl-")) {
            // <<query-dsl-match-query,match query>>
            // [`match`](/reference/query-languages/query-dsl/query-dsl-match-query.md)
            return makeLink(key, "query-dsl-", "/reference/query-languages/query-dsl/query-dsl-match-query.md");
        }
        // Known links to mapping-reference
        if (knownMapping.containsKey(parts[0])) {
            return makeLink(key, "", knownMapping.get(parts[0]));
        }
        // Various other remaining old asciidoc links
        // <<match-field-params,match query parameters>>
        return switch (parts[0]) {
            case "match-field-params" -> makeLink(key, "", "/reference/query-languages/query-dsl/query-dsl-match-query.md");
            case "search-aggregations-bucket-histogram-aggregation" -> makeLink(
                key,
                "",
                "/reference/aggregations/search-aggregations-bucket-histogram-aggregation.md"
            );
            default -> throw new IllegalArgumentException("Invalid link key <<" + key + ">>");
        };
    }

    private String parentFileFor(String cmd) {
        if (knownCommands.containsKey(cmd)) {
            return "/reference/query-languages/esql/commands/processing-commands.md";
        } else if (cmd.startsWith("mv_")) {
            return "/reference/query-languages/esql/functions-operators/mv-functions.md";
        } else if (cmd.contains("-operator")) {
            return "/reference/query-languages/esql/functions-operators/operators.md";
        } else if (cmd.startsWith("st_")) {
            return "/reference/query-languages/esql/functions-operators/spatial-functions.md";
        } else if (cmd.startsWith("to_")) {
            return "/reference/query-languages/esql/functions-operators/type-conversion-functions.md";
        } else if (cmd.startsWith("date_")) {
            return "/reference/query-languages/esql/functions-operators/date-time-functions.md";
        } else if (cmd.equals("split")) {
            return "/reference/query-languages/esql/functions-operators/string-functions.md";
        } else {
            return "/reference/query-languages/esql/functions-operators/aggregation-functions.md";
        }
    }

    private String makeLink(String key, String prefix, String parentFile) {
        String displayText = key.substring(prefix.length());
        if (knownCommands.containsKey(displayText)) {
            displayText = "`" + knownCommands.get(displayText).toUpperCase(Locale.ROOT) + "`";
        } else if (knownOperators.containsKey(displayText)) {
            displayText = "`" + knownOperators.get(displayText) + "`";
        } else {
            int comma = displayText.indexOf(',');
            if (comma > 0) {
                key = prefix + displayText.substring(0, comma);
                displayText = displayText.substring(comma + 1).trim();
            } else if (parentFile.contains("esql/esql-") || parentFile.contains("esql/functions-operators")) {
                // For ES|QL commands and functions we normally make uppercase code
                displayText = "`" + displayText.toUpperCase(Locale.ROOT) + "`";
            }
        }
        if (parentFile.contains("/" + key + ".md") || parentFile.contains("/" + key.replaceAll("esql-", "") + ".md")) {
            // The current docs-builder strips off all link targets that match the filename, so we need to do the same
            return String.format(Locale.ROOT, "[%s](%s)", displayText, parentFile);
        } else {
            return String.format(Locale.ROOT, "[%s](%s#%s)", displayText, parentFile, key);
        }
    }

    void writeToTempImageDir(String str) throws IOException {
        // We have to write to a tempdir because it’s all test are allowed to write to. Gradle can move them.
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("esql").resolve("images").resolve(category);
        tempFileWriter.writeToTempDir(dir, "svg", str);
    }

    void writeToTempSnippetsDir(String subdir, String str) throws IOException {
        // We have to write to a tempdir because it’s all test are allowed to write to. Gradle can move them.
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir"))
            .resolve("esql")
            .resolve("_snippets")
            .resolve(category)
            .resolve(subdir);
        tempFileWriter.writeToTempDir(dir, "md", str);
    }

    void writeToTempKibanaDir(String subdir, String extension, String str) throws IOException {
        // We have to write to a tempdir because it’s all test are allowed to write to. Gradle can move them.
        Path dir = PathUtils.get(System.getProperty("java.io.tmpdir")).resolve("esql").resolve("kibana").resolve(subdir).resolve(category);
        tempFileWriter.writeToTempDir(dir, extension, str);
    }

    protected abstract void renderSignature() throws IOException;

    protected abstract void renderDocs() throws Exception;

    static class FunctionDocsSupport extends DocsV3Support {
        private FunctionDocsSupport(String name, Class<?> testClass) {
            super("functions", name, testClass, () -> AbstractFunctionTestCase.signatures(testClass));
        }

        FunctionDocsSupport(
            String name,
            Class<?> testClass,
            FunctionDefinition definition,
            Supplier<Map<List<DataType>, DataType>> signatures
        ) {
            super("functions", name, definition, testClass, signatures);
        }

        @Override
        protected void renderSignature() throws IOException {
            String rendered = buildFunctionSignatureSvg();
            if (rendered == null) {
                logger.info("Skipping rendering signature because the function '{}' isn't registered", name);
            } else {
                logger.info("Writing function signature: {}", name);
                writeToTempImageDir(rendered);
            }
        }

        @Override
        protected void renderDocs() throws Exception {
            if (definition == null) {
                logger.info("Skipping rendering docs because the function '{}' isn't registered", name);
            } else {
                logger.info("Rendering function docs: {}", name);
                renderDocs(definition);
            }
        }

        private void renderDocs(FunctionDefinition definition) throws Exception {
            EsqlFunctionRegistry.FunctionDescription description = EsqlFunctionRegistry.description(definition);
            if (name.equals("case")) {
                /*
                 * Hack the description, so we render a proper one for case.
                 */
                // TODO build the description properly *somehow*
                EsqlFunctionRegistry.ArgSignature trueValue = description.args().get(1);
                EsqlFunctionRegistry.ArgSignature falseValue = new EsqlFunctionRegistry.ArgSignature(
                    "elseValue",
                    trueValue.type(),
                    "The value that’s returned when no condition evaluates to `true`.",
                    true,
                    true
                );
                description = new EsqlFunctionRegistry.FunctionDescription(
                    description.name(),
                    List.of(description.args().get(0), trueValue, falseValue),
                    description.returnType(),
                    description.description(),
                    description.variadic(),
                    description.type()
                );
            }
            renderTypes(name, description.args());
            renderParametersList(description.argNames(), description.argDescriptions());
            FunctionInfo info = EsqlFunctionRegistry.functionInfo(definition);
            assert info != null;
            renderDescription(description.description(), info.detailedDescription(), info.note());
            Optional<EsqlFunctionRegistry.ArgSignature> mapArgSignature = description.args()
                .stream()
                .filter(EsqlFunctionRegistry.ArgSignature::mapArg)
                .findFirst();
            boolean hasFunctionOptions = mapArgSignature.isPresent();
            if (hasFunctionOptions) {
                renderFunctionNamedParams((EsqlFunctionRegistry.MapArgSignature) mapArgSignature.get());
            }
            boolean hasExamples = renderExamples(info);
            boolean hasAppendix = renderAppendix(info.appendix());
            renderFullLayout(info, hasExamples, hasAppendix, hasFunctionOptions);
            renderKibanaInlineDocs(name, null, info);
            renderKibanaFunctionDefinition(name, null, info, description.args(), description.variadic());
        }

        private void renderFunctionNamedParams(EsqlFunctionRegistry.MapArgSignature mapArgSignature) throws IOException {
            StringBuilder rendered = new StringBuilder(DOCS_WARNING + """
                **Supported function named parameters**

                """);

            for (Map.Entry<String, EsqlFunctionRegistry.MapEntryArgSignature> argSignatureEntry : mapArgSignature.mapParams().entrySet()) {
                EsqlFunctionRegistry.MapEntryArgSignature arg = argSignatureEntry.getValue();
                rendered.append("`").append(arg.name()).append("`\n:   ");
                var type = arg.type().replaceAll("[\\[\\]]+", "");
                rendered.append("(").append(type).append(") ").append(arg.description()).append("\n\n");
            }

            logger.info("Writing function named parameters for [{}]:\n{}", name, rendered);
            writeToTempSnippetsDir("functionNamedParams", rendered.toString());
        }

        private String makeCallout(String type, String text) {
            return ":::{" + type + "}\n" + text.trim() + "\n:::\n";
        }

        private void appendLifeCycleAndVersion(StringBuilder appliesToText, FunctionAppliesTo appliesTo) {
            appliesToText.append(appliesTo.lifeCycle().name().toLowerCase(Locale.ROOT));
            if (appliesTo.version().isEmpty() == false) {
                appliesToText.append(" ").append(appliesTo.version());
            }
        }

        private String makeAppliesToText(FunctionAppliesTo[] functionAppliesTos) {
            StringBuilder appliesToText = new StringBuilder();
            if (functionAppliesTos.length > 0) {
                appliesToText.append("```{applies_to}\n");
                StringBuilder stackEntries = new StringBuilder();

                for (FunctionAppliesTo appliesTo : functionAppliesTos) {
                    if (stackEntries.isEmpty() == false) {
                        stackEntries.append(", ");
                    }
                    stackEntries.append(appliesTo.lifeCycle().name().toLowerCase(Locale.ROOT));
                    if (appliesTo.version().isEmpty() == false) {
                        stackEntries.append(" ").append(appliesTo.version());
                    }
                }

                // Add the stack entries
                if (stackEntries.isEmpty() == false) {
                    appliesToText.append("stack: ").append(stackEntries).append("\n");
                }

                // Add serverless entry only if there's a preview annotation
                boolean hasPreviewAnnotation = Arrays.stream(functionAppliesTos)
                    .anyMatch(appliesTo -> appliesTo.lifeCycle() == FunctionAppliesToLifecycle.PREVIEW);

                if (hasPreviewAnnotation) {
                    appliesToText.append("serverless: preview\n");
                }

                appliesToText.append("```\n");
            }
            return appliesToText.toString();
        }

        private void renderFullLayout(FunctionInfo info, boolean hasExamples, boolean hasAppendix, boolean hasFunctionOptions)
            throws IOException {
            String headingMarkdown = "#".repeat(2 + info.depthOffset());
            StringBuilder rendered = new StringBuilder(
                DOCS_WARNING + """
                    $HEAD$ `$UPPER_NAME$` [esql-$NAME$]
                    $APPLIES_TO$
                    **Syntax**

                    :::{image} ../../../images/$CATEGORY$/$NAME$.svg
                    :alt: Embedded
                    :class: text-center
                    :::

                    """.replace("$HEAD$", headingMarkdown)
                    .replace("$NAME$", name)
                    .replace("$CATEGORY$", category)
                    .replace("$UPPER_NAME$", name.toUpperCase(Locale.ROOT))
                    .replace("$APPLIES_TO$", makeAppliesToText(info.appliesTo()))
            );
            for (String section : new String[] { "parameters", "description", "types" }) {
                rendered.append(addInclude(section));
            }
            if (hasFunctionOptions) {
                rendered.append(addInclude("functionNamedParams"));
            }
            if (hasExamples) {
                rendered.append(addInclude("examples"));
            }
            if (hasAppendix) {
                rendered.append(addInclude("appendix"));
            }
            logger.info("Writing layout for [{}]:\n{}", name, rendered.toString());
            writeToTempSnippetsDir("layout", rendered.toString());
        }

        private String addInclude(String section) {
            return """

                :::{include} ../$SECTION$/$NAME$.md
                :::
                """.replace("$NAME$", name).replace("$SECTION$", section);
        }
    }

    /** Operator specific docs generating, since it is currently quite different from the function docs generating */
    public static class OperatorsDocsSupport extends DocsV3Support {
        private final OperatorConfig op;

        private OperatorsDocsSupport(String name, Class<?> testClass) {
            this(name, testClass, OPERATORS.get(name), () -> AbstractFunctionTestCase.signatures(testClass));
        }

        public OperatorsDocsSupport(
            String name,
            Class<?> testClass,
            OperatorConfig op,
            Supplier<Map<List<DataType>, DataType>> signatures
        ) {
            super("operators", name, testClass, signatures);
            this.op = op;
        }

        @Override
        public void renderSignature() throws IOException {
            String rendered = (switch (op.category()) {
                case BINARY -> RailRoadDiagram.infixOperator("lhs", op.symbol(), "rhs");
                case UNARY -> RailRoadDiagram.prefixOperator(op.symbol(), "v");
                case SEARCH -> RailRoadDiagram.infixOperator("field", op.symbol(), "query");
                case NULL_PREDICATES -> RailRoadDiagram.suffixOperator("field", op.symbol());
                case IN -> RailRoadDiagram.infixOperator("field", op.symbol(), "values");
                case LIKE_AND_RLIKE -> RailRoadDiagram.infixOperator("field", op.symbol(), "pattern");
                case CAST -> RailRoadDiagram.infixOperator("field", op.symbol(), "type");
                default -> buildFunctionSignatureSvg();
            });
            if (rendered == null) {
                logger.info("Skipping rendering signature because the operator isn't registered");
            } else {
                logger.info("Writing operator signature");
                writeToTempImageDir(rendered);
            }
        }

        @Override
        public void renderDocs() throws Exception {
            Constructor<?> ctor = constructorWithFunctionInfo(op.clazz());
            if (ctor != null) {
                FunctionInfo functionInfo = ctor.getAnnotation(FunctionInfo.class);
                assert functionInfo != null;
                renderDocsForOperators(op.name(), op.titleName(), ctor, functionInfo, op.variadic());
            } else {
                logger.info("Skipping rendering docs for operator '" + op.name() + "' with no @FunctionInfo");
            }
        }

        void renderDocsForNegatedOperators(Constructor<?> ctor, Function<String, String> description) throws Exception {
            String baseName = name.toLowerCase(Locale.ROOT).replace("not ", "");
            OperatorConfig op = OPERATORS.get(baseName);
            assert op != null;
            FunctionInfo orig = ctor.getAnnotation(FunctionInfo.class);
            assert orig != null;
            FunctionInfo functionInfo = new FunctionInfo() {
                @Override
                public Class<? extends Annotation> annotationType() {
                    return orig.annotationType();
                }

                @Override
                public String operator() {
                    return name;
                }

                @Override
                public String[] returnType() {
                    return orig.returnType();
                }

                @Override
                public boolean preview() {
                    return orig.preview();
                }

                @Override
                public FunctionAppliesTo[] appliesTo() {
                    return orig.appliesTo();
                }

                @Override
                public String description() {
                    return description.apply(orig.description().replace(baseName, name));
                }

                @Override
                public String detailedDescription() {
                    return "";
                }

                @Override
                public String note() {
                    return orig.note().replace(baseName, name);
                }

                @Override
                public String appendix() {
                    return orig.appendix().replace(baseName, name);
                }

                @Override
                public int depthOffset() {
                    return orig.depthOffset();
                }

                @Override
                public FunctionType type() {
                    return orig.type();
                }

                @Override
                public Example[] examples() {
                    // throw away examples
                    return new Example[] {};
                }
            };
            String name = "not_" + baseName;
            renderDocsForOperators(name, null, ctor, functionInfo, op.variadic());
        }

        void renderDocsForOperators(String name, String titleName, Constructor<?> ctor, FunctionInfo info, boolean variadic)
            throws Exception {
            renderKibanaInlineDocs(name, titleName, info);

            var params = ctor.getParameters();

            List<EsqlFunctionRegistry.ArgSignature> args = new ArrayList<>(params.length);
            for (int i = 1; i < params.length; i++) { // skipping 1st argument, the source
                if (Configuration.class.isAssignableFrom(params[i].getType()) == false) {
                    MapParam mapParamInfo = params[i].getAnnotation(MapParam.class);
                    if (mapParamInfo != null) {
                        args.add(mapParam(mapParamInfo));
                    } else {
                        Param paramInfo = params[i].getAnnotation(Param.class);
                        args.add(paramInfo != null ? param(paramInfo, false) : paramWithoutAnnotation(params[i].getName()));
                    }
                }
            }
            renderKibanaFunctionDefinition(name, titleName, info, args, variadic);
            renderDetailedDescription(info.detailedDescription(), info.note());
            renderTypes(name, args);
            renderExamples(info);
            // TODO: Consider rendering more for operators, like layout, which is currently static
        }

        void renderDetailedDescription(String detailedDescription, String note) throws IOException {
            StringBuilder rendered = new StringBuilder();
            if (Strings.isNullOrEmpty(detailedDescription) == false) {
                detailedDescription = replaceLinks(detailedDescription.trim());
                rendered.append(DOCS_WARNING).append(detailedDescription).append("\n");
            }

            if (Strings.isNullOrEmpty(note) == false) {
                rendered.append("\n::::{note}\n").append(replaceLinks(note)).append("\n::::\n\n");
            }
            if (rendered.isEmpty() == false) {
                rendered.append("\n");
                logger.info("Writing detailed description for [{}]:\n{}", name, rendered);
                writeToTempSnippetsDir("detailedDescription", rendered.toString());
            }
        }
    }

    /** Command specific docs generating, currently very empty since we only render kibana definition files */
    public static class CommandsDocsSupport extends DocsV3Support {
        private final LogicalPlan command;
        private final XPackLicenseState licenseState;

        public CommandsDocsSupport(String name, Class<?> testClass, LogicalPlan command, XPackLicenseState licenseState) {
            super("commands", name, testClass, Map::of);
            this.command = command;
            this.licenseState = licenseState;
        }

        @Override
        public void renderSignature() throws IOException {
            // Unimplemented until we make command docs dynamically generated
        }

        @Override
        public void renderDocs() throws Exception {
            // Currently we only render kibana definition files, but we could expand to rendering much more if we decide to
            renderKibanaCommandDefinition();
        }

        void renderKibanaCommandDefinition() throws Exception {
            try (XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().lfAtEnd().startObject()) {
                builder.field(
                    "comment",
                    "This is generated by ESQL’s DocsV3Support. Do no edit it. See ../README.md for how to regenerate it."
                );
                builder.field("type", "command");
                builder.field("name", name);
                License.OperationMode license = licenseState.getOperationMode();
                if (license != null && license != License.OperationMode.BASIC) {
                    builder.field("license", license.toString());
                }
                String rendered = Strings.toString(builder.endObject());
                logger.info("Writing kibana command definition for [{}]:\n{}", name, rendered);
                writeToTempKibanaDir("definition", "json", rendered);
            }
        }
    }

    protected String buildFunctionSignatureSvg() throws IOException {
        return (definition != null) ? RailRoadDiagram.functionSignature(definition) : null;
    }

    void renderParametersList(List<String> argNames, List<String> argDescriptions) throws IOException {
        StringBuilder builder = new StringBuilder();
        builder.append(DOCS_WARNING);
        builder.append("**Parameters**\n");
        for (int a = 0; a < argNames.size(); a++) {
            String description = replaceLinks(argDescriptions.get(a));
            builder.append("\n`").append(argNames.get(a)).append("`\n:   ").append(description).append('\n');
        }
        builder.append('\n');
        String rendered = builder.toString();
        logger.info("Writing parameters for [{}]:\n{}", name, rendered);
        writeToTempSnippetsDir("parameters", rendered);
    }

    void renderTypes(String name, List<EsqlFunctionRegistry.ArgSignature> args) throws IOException {
        StringBuilder header = new StringBuilder("| ");
        StringBuilder separator = new StringBuilder("| ");
        List<String> argNames = args.stream().map(EsqlFunctionRegistry.ArgSignature::name).toList();
        for (String arg : argNames) {
            header.append(arg).append(" | ");
            separator.append("---").append(" | ");
        }
        header.append("result |");
        separator.append("--- |");

        List<String> table = new ArrayList<>();
        for (Map.Entry<List<DataType>, DataType> sig : this.signatures.get().entrySet()) { // TODO flip to using sortedSignatures
            if (shouldHideSignature(sig.getKey(), sig.getValue())) {
                continue;
            }
            if (sig.getKey().size() > argNames.size()) { // skip variadic [test] cases (but not those with optional parameters)
                continue;
            }
            table.add(getTypeRow(args, sig, argNames));
        }
        Collections.sort(table);
        if (table.isEmpty()) {
            logger.info("Warning: No table of types generated for [{}]", name);
            return;
        }

        String rendered = DOCS_WARNING + """
            **Supported types**

            """ + header + "\n" + separator + "\n" + String.join("\n", table) + "\n\n";
        logger.info("Writing function types for [{}]:\n{}", name, rendered);
        writeToTempSnippetsDir("types", rendered);
    }

    private static String getTypeRow(
        List<EsqlFunctionRegistry.ArgSignature> args,
        Map.Entry<List<DataType>, DataType> sig,
        List<String> argNames
    ) {
        StringBuilder b = new StringBuilder("| ");
        for (int i = 0; i < sig.getKey().size(); i++) {
            DataType argType = sig.getKey().get(i);
            EsqlFunctionRegistry.ArgSignature argSignature = args.get(i);
            if (argSignature.mapArg()) {
                b.append("named parameters");
            } else {
                b.append(argType.esNameIfPossible());
            }
            b.append(" | ");
        }
        b.append("| ".repeat(argNames.size() - sig.getKey().size()));
        b.append(sig.getValue().esNameIfPossible());
        b.append(" |");
        return b.toString();
    }

    void renderDescription(String description, String detailedDescription, String note) throws IOException {
        description = replaceLinks(description.trim());
        note = replaceLinks(note);
        String rendered = DOCS_WARNING + """
            **Description**

            """ + description + "\n";

        detailedDescription = replaceLinks(detailedDescription.trim());
        if (Strings.isNullOrEmpty(detailedDescription) == false) {
            rendered += "\n" + detailedDescription + "\n";
        }

        if (Strings.isNullOrEmpty(note) == false) {
            rendered += "\n::::{note}\n" + note + "\n::::\n\n";
        }
        rendered += "\n";
        logger.info("Writing description for [{}]:\n{}", name, rendered);
        writeToTempSnippetsDir("description", rendered);
    }

    protected boolean renderExamples(FunctionInfo info) throws IOException {
        if (info == null || info.examples().length == 0) {
            return false;
        }
        StringBuilder builder = new StringBuilder();
        builder.append(DOCS_WARNING);
        if (info.examples().length == 1) {
            builder.append("**Example**\n\n");
        } else {
            builder.append("**Examples**\n\n");
        }
        for (Example example : info.examples()) {
            if (example.applies_to().isEmpty() == false) {
                builder.append("```{applies_to}\n");
                builder.append(example.applies_to()).append("\n");
                builder.append("```\n\n");
            }
            if (example.description().isEmpty() == false) {
                builder.append(replaceLinks(example.description().trim()));
                builder.append("\n\n");
            }
            String exampleQuery = loadExampleQuery(example);
            String exampleResult = loadExampleResult(example);
            builder.append(exampleQuery).append("\n");
            if (exampleResult != null && exampleResult.isEmpty() == false) {
                builder.append(exampleResult).append("\n");
            }
            if (example.explanation().isEmpty() == false) {
                builder.append("\n");
                builder.append(replaceLinks(example.explanation().trim()));
                builder.append("\n\n");
            }
        }
        builder.append('\n');
        String rendered = builder.toString();
        logger.info("Writing examples for [{}]:\n{}", name, rendered);
        writeToTempSnippetsDir("examples", rendered);
        return true;
    }

    void renderKibanaInlineDocs(String name, String titleName, FunctionInfo info) throws IOException {
        titleName = titleName == null ? name.replace("_", " ") : titleName;
        if (false == info.operator().isEmpty()
            && false == titleName.toUpperCase(Locale.ROOT).replaceAll("_", " ").equals(info.operator().toUpperCase(Locale.ROOT))) {
            titleName = titleName + " `" + info.operator() + "`";
        }
        StringBuilder builder = new StringBuilder();
        builder.append(DOCS_WARNING);
        builder.append("### ").append(titleName.toUpperCase(Locale.ROOT)).append("\n");
        builder.append(replaceLinks(info.description())).append("\n\n");

        if (info.examples().length > 0) {
            Example example = info.examples()[0];
            builder.append("```esql\n");
            builder.append(loadExample(example.file(), example.tag()));
            builder.append("\n```\n");
        }
        if (Strings.isNullOrEmpty(info.note()) == false) {
            builder.append("Note: ").append(replaceLinks(info.note())).append("\n");
        }
        String rendered = builder.toString();
        logger.info("Writing kibana inline docs for [{}]:\n{}", name, rendered);
        writeToTempKibanaDir("docs", "md", rendered);
    }

    void renderKibanaFunctionDefinition(
        String name,
        String titleName,
        FunctionInfo info,
        List<EsqlFunctionRegistry.ArgSignature> args,
        boolean variadic
    ) throws Exception {

        try (XContentBuilder builder = JsonXContent.contentBuilder().prettyPrint().lfAtEnd().startObject()) {
            builder.field(
                "comment",
                "This is generated by ESQL’s AbstractFunctionTestCase. Do no edit it. See ../README.md for how to regenerate it."
            );
            if (false == info.operator().isEmpty()) {
                builder.field("type", "operator");
                builder.field("operator", info.operator());
                // TODO: Bring back this assertion
                // assertThat(isAggregation(), equalTo(false));
            } else {
                builder.field("type", switch (info.type()) {
                    case SCALAR -> "scalar";
                    case AGGREGATE -> "agg";
                    case GROUPING -> "grouping";
                });
            }
            builder.field("name", name);
            License.OperationMode license = licenseChecker.invoke(null);
            if (license != null && license != License.OperationMode.BASIC) {
                builder.field("license", license.toString());
            }
            if (titleName != null && titleName.equals(name) == false) {
                builder.field("titleName", titleName);
            }
            builder.field("description", removeAsciidocLinks(info.description()));
            if (Strings.isNullOrEmpty(info.note()) == false) {
                builder.field("note", removeAsciidocLinks(info.note()));
            }
            // TODO aliases

            builder.startArray("signatures");
            if (args.isEmpty()) {
                builder.startObject();
                builder.startArray("params");
                builder.endArray();
                // There should only be one return type so just use that as the example
                builder.field("returnType", signatures.get().values().iterator().next().esNameIfPossible());
                builder.endObject();
            } else {
                int minArgCount = (int) args.stream().filter(a -> false == a.optional()).count();
                for (Map.Entry<List<DataType>, DataType> sig : sortedSignatures()) {
                    if (variadic && sig.getKey().size() > args.size()) {
                        // For variadic functions we test much longer signatures, let’s just stop at the last one
                        continue;
                    }
                    if (sig.getKey().size() < minArgCount) {
                        throw new IllegalArgumentException("signature " + sig.getKey() + " is missing non-optional arg for " + args);
                    }
                    if (shouldHideSignature(sig.getKey(), sig.getValue())) {
                        continue;
                    }
                    builder.startObject();
                    builder.startArray("params");
                    for (int i = 0; i < sig.getKey().size(); i++) {
                        EsqlFunctionRegistry.ArgSignature arg = args.get(i);
                        builder.startObject();
                        builder.field("name", arg.name());
                        if (arg.mapArg()) {
                            builder.field("type", "function_named_parameters");
                            builder.field(
                                "mapParams",
                                arg.mapParams()
                                    .values()
                                    .stream()
                                    .map(mapArgSignature -> "{" + mapArgSignature + "}")
                                    .collect(Collectors.joining(", "))
                            );
                        } else {
                            builder.field("type", sig.getKey().get(i).esNameIfPossible());
                        }
                        builder.field("optional", arg.optional());
                        builder.field("description", arg.description());
                        builder.endObject();
                    }
                    builder.endArray();
                    license = licenseChecker.invoke(sig.getKey());
                    if (license != null && license != License.OperationMode.BASIC) {
                        builder.field("license", license.toString());
                    }
                    builder.field("variadic", variadic);
                    builder.field("returnType", sig.getValue().esNameIfPossible());
                    builder.endObject();
                }
            }
            builder.endArray();

            if (info.examples().length > 0) {
                builder.startArray("examples");
                for (Example example : info.examples()) {
                    builder.value(loadExample(example.file(), example.tag()));
                }
                builder.endArray();
            }
            builder.field("preview", info.preview());
            builder.field("snapshot_only", EsqlFunctionRegistry.isSnapshotOnly(name));

            String rendered = Strings.toString(builder.endObject());
            logger.info("Writing kibana function definition for [{}]:\n{}", name, rendered);
            writeToTempKibanaDir("definition", "json", rendered);
        }
    }

    private String removeAsciidocLinks(String asciidoc) {
        // Some docs have asciidoc links while others use the newer markdown, so we need to first replace asciidoc with markdown
        String md = replaceLinks(asciidoc);
        // Now replace the markdown with just the display text
        return md.replaceAll("\\[(\\s*`?[^]]+?`?\\s*)]\\(([^()\\s]+(?:\\([^()]*\\)[^()]*)*)\\)", "$1");
    }

    private List<Map.Entry<List<DataType>, DataType>> sortedSignatures() {
        List<Map.Entry<List<DataType>, DataType>> sortedSignatures = new ArrayList<>(signatures.get().entrySet());
        sortedSignatures.sort((lhs, rhs) -> {
            int maxlen = Math.max(lhs.getKey().size(), rhs.getKey().size());
            for (int i = 0; i < maxlen; i++) {
                if (lhs.getKey().size() <= i) {
                    return -1;
                }
                if (rhs.getKey().size() <= i) {
                    return 1;
                }
                int c = lhs.getKey().get(i).esNameIfPossible().compareTo(rhs.getKey().get(i).esNameIfPossible());
                if (c != 0) {
                    return c;
                }
            }
            return lhs.getValue().esNameIfPossible().compareTo(rhs.getValue().esNameIfPossible());
        });
        return sortedSignatures;
    }

    protected boolean renderAppendix(String appendix) throws IOException {
        if (appendix.isEmpty()) {
            return false;
        }

        String rendered = DOCS_WARNING + replaceLinks(appendix) + "\n";

        logger.info("Writing appendix for [{}]:\n{}", name, rendered);
        writeToTempSnippetsDir("appendix", rendered);
        return true;
    }

    private final HashMap<String, Map<String, String>> examples = new HashMap<>();

    protected String loadExampleQuery(Example example) throws IOException {
        return "```esql\n" + loadExample(example.file(), example.tag()) + "\n```\n";
    }

    protected String loadExampleResult(Example example) throws IOException {
        return loadExample(example.file(), example.tag() + "-result");
    }

    protected String loadExample(String csvSpec, String tag) throws IOException {
        if (examples.containsKey(csvSpec) == false) {
            var taggedExamples = loadExampleFile(csvSpec);
            examples.put(csvSpec, taggedExamples);
            return taggedExamples.get(tag);
        } else {
            return examples.get(csvSpec).get(tag);
        }
    }

    protected Map<String, String> loadExampleFile(String csvSpec) throws IOException {
        Map<String, String> taggedExamples = new HashMap<>();
        String csvFile = csvSpec.endsWith(".csv-spec") ? csvSpec : csvSpec + ".csv-spec";
        InputStream specInputStream = CsvTestsDataLoader.class.getResourceAsStream("/" + csvFile);
        if (specInputStream == null) {
            throw new IllegalArgumentException("Failed to find examples file [" + csvFile + "]");
        }
        try (BufferedReader spec = new BufferedReader(new InputStreamReader(specInputStream, StandardCharsets.UTF_8))) {

            String line;
            Pattern tagPattern = Pattern.compile("//\\s*(tag|end)::([\\w\\-_]+)\\[]");

            String currentTag = null;
            List<String> currentLines = null;

            while ((line = spec.readLine()) != null) {
                Matcher tagMatcher = tagPattern.matcher(line);
                if (tagMatcher.find()) {
                    String tagType = tagMatcher.group(1);
                    currentTag = tagMatcher.group(2);
                    if (tagType.equals("tag")) {
                        currentLines = new ArrayList<>();
                    } else if (tagType.equals("end")) {
                        if (currentLines == null) {
                            throw new IllegalArgumentException("End tag found, but no starting tag was found: " + line);
                        }
                        taggedExamples.put(currentTag, reformatExample(currentTag, currentLines));
                        currentLines = null;
                        currentTag = null;
                    }
                } else if (currentTag != null && currentLines != null) {
                    currentLines.add(line); // Collect lines within the block
                }
            }
        }
        return taggedExamples;
    }

    protected String reformatExample(String tag, List<String> lines) {
        if (tag.endsWith("-result")) {
            StringBuilder sb = new StringBuilder();
            for (String line : lines) {
                sb.append(renderTableLine(line, sb.isEmpty()));
            }
            return sb.toString();
        } else {
            return lines.stream().map(l -> l.stripTrailing().replaceAll("\\\\", "\\\\\\\\")).collect(Collectors.joining("\n"));
        }
    }

    private String renderTableLine(String line, boolean header) {
        String[] columns = line.split("\\|");
        if (header) {
            return renderTableLine(columns) + renderTableSpacerLine(columns.length);
        } else {
            return renderTableLine(columns);
        }
    }

    private String renderTableLine(String[] columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("| ");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sb.append(" | ");
            }
            // Some cells have regex content (see CATEGORIZE), so we need to escape this
            sb.append(columns[i].trim().replaceAll("\\.\\*", ".\\\\*"));
        }
        return sb.append(" |\n").toString();
    }

    private String renderTableSpacerLine(int columns) {
        StringBuilder sb = new StringBuilder();
        sb.append("| ");
        for (int i = 0; i < columns; i++) {
            if (i > 0) {
                sb.append(" | ");
            }
            sb.append("---");
        }
        return sb.append(" |\n").toString();
    }
}
