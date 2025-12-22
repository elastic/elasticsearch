/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash.CategorizeDef;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash.CategorizeDef.OutputFormat;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.LicenseAware;
import org.elasticsearch.xpack.esql.SupportsObservabilityTier;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ExpressionContext;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.FunctionType;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;

import static java.util.Map.entry;
import static org.elasticsearch.common.logging.LoggerMessageFormat.format;
import static org.elasticsearch.compute.aggregation.blockhash.BlockHash.CategorizeDef.OutputFormat.REGEX;
import static org.elasticsearch.xpack.esql.SupportsObservabilityTier.ObservabilityTier.COMPLETE;
import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;

/**
 * Categorizes text messages.
 * <p>
 *     This function has no evaluators, as it works like an aggregation (Accumulates values, stores intermediate states, etc).
 * </p>
 * <p>
 *     For the implementation, see {@link org.elasticsearch.compute.aggregation.blockhash.CategorizeBlockHash}
 * </p>
 */
@SupportsObservabilityTier(tier = COMPLETE)
public class Categorize extends GroupingFunction.NonEvaluatableGroupingFunction implements OptionalArgument, LicenseAware {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "Categorize",
        Categorize::new
    );
    private static final TransportVersion ESQL_CATEGORIZE_OPTIONS = TransportVersion.fromName("esql_categorize_options");

    private static final String ANALYZER = "analyzer";
    private static final String OUTPUT_FORMAT = "output_format";
    private static final String SIMILARITY_THRESHOLD = "similarity_threshold";

    private static final Map<String, DataType> ALLOWED_OPTIONS = new TreeMap<>(
        Map.ofEntries(entry(ANALYZER, KEYWORD), entry(OUTPUT_FORMAT, KEYWORD), entry(SIMILARITY_THRESHOLD, INTEGER))
    );

    private final Expression field;
    private final Expression options;

    @FunctionInfo(
        returnType = "keyword",
        description = "Groups text messages into categories of similarly formatted text values.",
        detailedDescription = """
            `CATEGORIZE` has the following limitations:

            * can’t be used within other expressions
            * can’t be used more than once in the groupings
            * can’t be used or referenced within aggregate functions and it has to be the first grouping""",
        examples = {
            @Example(
                file = "docs",
                tag = "docsCategorize",
                description = "This example categorizes server logs messages into categories and aggregates their counts. "
            ) },
        type = FunctionType.GROUPING,
        appliesTo = {
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.0"),
            @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.GA, version = "9.1") }
    )
    public Categorize(
        Source source,
        @Param(name = "field", type = { "text", "keyword" }, description = "Expression to categorize") Expression field,
        @MapParam(
            name = "options",
            description = "(Optional) Categorize additional options as "
                + "<<esql-function-named-params,function named parameters>>. "
                + "{applies_to}`stack: ga 9.2`}",
            params = {
                @MapParam.MapParamEntry(
                    name = ANALYZER,
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "Analyzer used to convert the field into tokens for text categorization."
                ),
                @MapParam.MapParamEntry(
                    name = OUTPUT_FORMAT,
                    type = "keyword",
                    valueHint = { "regex", "tokens" },
                    description = "The output format of the categories. Defaults to regex."
                ),
                @MapParam.MapParamEntry(
                    name = SIMILARITY_THRESHOLD,
                    type = "integer",
                    valueHint = { "70" },
                    description = "The minimum percentage of token weight that must match for text to be added to the category bucket. "
                        + "Must be between 1 and 100. The larger the value the narrower the categories. "
                        + "Larger values will increase memory usage and create narrower categories. Defaults to 70."
                ), },
            optional = true
        ) Expression options
    ) {
        super(source, options == null ? List.of(field) : List.of(field, options));
        this.field = field;
        this.options = options;
    }

    private Categorize(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.getTransportVersion().supports(ESQL_CATEGORIZE_OPTIONS) ? in.readOptionalNamedWriteable(Expression.class) : null
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        if (out.getTransportVersion().supports(ESQL_CATEGORIZE_OPTIONS)) {
            out.writeOptionalNamedWriteable(options);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public boolean foldable() {
        // Categorize cannot be currently folded
        return false;
    }

    @Override
    public Nullability nullable() {
        // Null strings and strings that don’t produce tokens after analysis lead to null values.
        // This includes empty strings, only whitespace, (hexa)decimal numbers and stopwords.
        return Nullability.TRUE;
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT).and(
            Options.resolve(options, source(), SECOND, ALLOWED_OPTIONS, this::verifyOptions)
        );
    }

    private void verifyOptions(Map<String, Object> optionsMap) {
        if (options == null) {
            return;
        }
        Integer similarityThreshold = (Integer) optionsMap.get(SIMILARITY_THRESHOLD);
        if (similarityThreshold != null) {
            if (similarityThreshold <= 0 || similarityThreshold > 100) {
                throw new InvalidArgumentException(
                    format("invalid similarity threshold [{}], expecting a number between 1 and 100, inclusive", similarityThreshold)
                );
            }
        }
        String outputFormat = (String) optionsMap.get(OUTPUT_FORMAT);
        if (outputFormat != null) {
            try {
                OutputFormat.valueOf(outputFormat.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new InvalidArgumentException(
                    format(null, "invalid output format [{}], expecting one of [REGEX, TOKENS]", outputFormat)
                );
            }
        }
    }

    public CategorizeDef categorizeDef() {
        Map<String, Object> optionsMap = new HashMap<>();
        if (options != null) {
            Options.populateMap((MapExpression) options, optionsMap, source(), SECOND, ALLOWED_OPTIONS);
        }
        Integer similarityThreshold = (Integer) optionsMap.get(SIMILARITY_THRESHOLD);
        String outputFormatString = (String) optionsMap.get(OUTPUT_FORMAT);
        OutputFormat outputFormat = outputFormatString == null ? null : OutputFormat.valueOf(outputFormatString.toUpperCase(Locale.ROOT));
        return new CategorizeDef(
            (String) optionsMap.get("analyzer"),
            outputFormat == null ? REGEX : outputFormat,
            similarityThreshold == null ? 70 : similarityThreshold
        );
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    public Categorize replaceChildren(List<Expression> newChildren) {
        return new Categorize(source(), newChildren.get(0), newChildren.size() > 1 ? newChildren.get(1) : null);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, Categorize::new, field, options);
    }

    public Expression field() {
        return field;
    }

    @Override
    public String toString() {
        return "Categorize{field=" + field + "}";
    }

    @Override
    public boolean licenseCheck(XPackLicenseState state) {
        return MachineLearning.CATEGORIZE_TEXT_AGG_FEATURE.check(state);
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification(ExpressionContext ctx) {
        return (p, failures) -> {
            super.postAnalysisPlanVerification(ctx).accept(p, failures);

            if (p instanceof InlineStats inlineStats && inlineStats.child() instanceof Aggregate aggregate) {
                aggregate.groupings().forEach(grp -> {
                    if (grp instanceof Alias alias && alias.child() instanceof Categorize categorize) {
                        failures.add(
                            fail(
                                categorize,
                                "CATEGORIZE [{}] is not yet supported with INLINE STATS [{}]",
                                categorize.sourceText(),
                                inlineStats.sourceText()
                            )
                        );
                    }
                });
            }
        };
    }
}
