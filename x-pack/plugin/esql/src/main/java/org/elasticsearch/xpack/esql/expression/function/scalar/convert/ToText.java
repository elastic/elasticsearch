/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.MapParam;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;

/**
 * Converts a value to an expression of type {@code TEXT}. This is different from the {@link ToString} function, which converts to
 * {@code KEYWORD}. {@code TEXT} and {@code KEYWORD} data types are treated in ES|QL almost the same, the main difference is that
 * {@code TEXT} is considered to be analyzed, while {@code KEYWORD} is not.
 * This matters for functions like {@link org.elasticsearch.xpack.esql.expression.function.fulltext.Match} which will treat these data types
 * differently.
 * <p>
 * The optional {@code analyzer} option declares how the values of the resulting column are analyzed, playing the role the mapping's
 * {@code analyzer} plays for an indexed text field. It is only accepted on expressions that are not backed by an index-mapped field:
 * for an indexed field the mapping already made that declaration, and honoring an override would force row-by-row re-analysis instead
 * of searching the index, thereby preventing pushdown and incurring a massive performance penalty - allowing this would be a massive
 * footgun, hence the rejection (with explicit error message).  But we could decide to support this in the future if users really want it.
 * <p>
 * The name must be a registered (prebuilt or plugin-contributed) analyzer; per-index custom analyzers are not resolvable because the
 * expression is not backed by an index.
 */
public class ToText extends AbstractConvertFunction
    implements
        EvaluatorMapper,
        OptionalArgument,
        PostAnalysisPlanVerificationAware,
        AnalyzedTextExpression {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(Expression.class, "ToText", ToText::new);
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToText.class)
        .binary(ToText::new)
        .capabilities("analyzer")
        .name("to_text");

    public static final TransportVersion ESQL_TO_TEXT_VALUES_ANALYZER = TransportVersion.fromName("esql_to_text_values_analyzer");

    private static final String ANALYZER = "analyzer";
    public static final Map<String, DataType> ALLOWED_OPTIONS = Map.ofEntries(Map.entry(ANALYZER, KEYWORD));

    private static final Map<DataType, BuildFactory> EVALUATORS = Map.ofEntries(
        Map.entry(KEYWORD, (source, fieldEval) -> fieldEval),
        Map.entry(TEXT, (source, fieldEval) -> fieldEval)
    );

    private final Expression options;

    public ToText(Source source, Expression v) {
        this(source, v, null);
    }

    @FunctionInfo(
        returnType = "text",
        briefSummary = "Converts a value to text.",
        description = "Converts an input value into text.",
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.5.0") },
        preview = true,
        examples = { @Example(file = "convert", tag = "to_text") }
    )
    public ToText(
        Source source,
        @Param(
            name = "field",
            type = { "keyword", "text" },
            description = "Input value. The input can be a single- or multi-valued column or an expression."
        ) Expression field,
        @MapParam(
            name = "options",
            params = {
                @MapParam.MapParamEntry(
                    name = "analyzer",
                    type = "keyword",
                    valueHint = { "standard" },
                    description = "The analyzer applied to the values of the resulting text column, playing the role the "
                        + "mapping's `analyzer` plays for an indexed text field. Defaults to `standard`. Must name a registered "
                        + "(prebuilt or plugin-provided) analyzer, and is only accepted on expressions that are not backed by an "
                        + "index-mapped field."
                ) },
            description = "(Optional) Additional options as <<esql-function-named-params,function named parameters>>.",
            optional = true
        ) Expression options
    ) {
        super(source, field);
        this.options = options;
    }

    private ToText(StreamInput in) throws IOException {
        super(in);
        this.options = in.getTransportVersion().supports(ESQL_TO_TEXT_VALUES_ANALYZER)
            ? in.readOptionalNamedWriteable(Expression.class)
            : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (out.getTransportVersion().supports(ESQL_TO_TEXT_VALUES_ANALYZER)) {
            out.writeOptionalNamedWriteable(options);
        } else if (options != null) {
            // Writing the field without its options would silently change how the column's values are analyzed.
            // IllegalArgumentException returns a 400 to the user, which is what we want here.
            throw new IllegalArgumentException(
                "["
                    + sourceText()
                    + "] with options is not supported in peer node's version ["
                    + out.getTransportVersion()
                    + "]. Upgrade to version ["
                    + ESQL_TO_TEXT_VALUES_ANALYZER
                    + "] or newer."
            );
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public Expression options() {
        return options;
    }

    /**
     * The declared values analyzer name, or {@code null} when no {@code analyzer} option was given.
     */
    @Override
    public String valuesAnalyzer() {
        if (options instanceof MapExpression map && map.keyFoldedMap().get(ANALYZER) instanceof Literal literal) {
            return BytesRefs.toString(literal.value());
        }
        return null;
    }

    @Override
    protected TypeResolution resolveType() {
        return super.resolveType().and(Options.resolve(options, source(), SECOND, ALLOWED_OPTIONS));
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return postAnalysisPlanVerification(null);
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification(AnalysisRegistry analysisRegistry) {
        return (plan, failures) -> {
            String analyzerName = valuesAnalyzer();
            // Every checker runs against every plan node; only act for the node holding this instance.
            if (analyzerName == null || isInCurrentNode(plan) == false) {
                return;
            }
            // The registry is only available in the post-analysis pass; analyzer names cannot change during
            // optimization, so the post-optimization call with a null registry will skip this check.
            if (analysisRegistry != null) {
                try {
                    PlannerUtils.resolveAnalyzer(analyzerName, analysisRegistry);
                } catch (InvalidArgumentException e) {
                    failures.add(fail(this, "{}", e.getMessage()));
                }
            }
            if (resolvesToMappedFieldAttribute(plan, field())) {
                failures.add(fail(field(), "{}", analyzerOnMappedFieldMessage(field().sourceText())));
            }
        };
    }

    /**
     * The error for declaring the {@code analyzer} option over an index-mapped field. Shared between the
     * post-analysis verifier and the analyzer's union-type resolution, which encounters (and must reject)
     * the same situation before the verifier ever runs.
     */
    public static String analyzerOnMappedFieldMessage(String fieldName) {
        return "[analyzer] option is not supported for [TO_TEXT] on index-mapped field ["
            + fieldName
            + "]: it would require re-analyzing values row by row rather than searching the index, likely a major and unintended "
            + "performance degradation";
    }

    private boolean isInCurrentNode(LogicalPlan plan) {
        return plan.expressions().stream().anyMatch(e -> e.anyMatch(c -> c == this));
    }

    /**
     * Whether {@code input} is an index-mapped {@link FieldAttribute}, possibly through one or more
     * {@code RENAME}/{@code Project} or aliasing {@code EVAL} chains in {@code plan}. Those are exactly the inputs a
     * full-text function can push down to Lucene (once optimization collapses the aliases — aliasing {@code EVAL}s
     * become projections through {@code ReplaceAliasingEvalWithProject}), so they are the inputs where declaring a
     * values analyzer must be rejected. Inputs that are runtime either way — computed expressions, potentially
     * unmapped fields, federated-source columns, {@code MV_EXPAND}/{@code FORK} outputs — are legitimate declaration
     * sites and resolve to {@code false}.
     */
    private static boolean resolvesToMappedFieldAttribute(LogicalPlan plan, Expression input) {
        if (input instanceof FieldAttribute fa) {
            return isMapped(fa);
        }
        if (input instanceof Attribute == false) {
            return false;
        }
        Holder<Attribute> current = new Holder<>((Attribute) input);
        Holder<Boolean> mapped = new Holder<>(false);
        plan.forEachDownMayReturnEarly((p, breakEarly) -> {
            List<? extends NamedExpression> aliases;
            if (p instanceof Project project) {
                aliases = project.projections();
            } else if (p instanceof Eval eval) {
                aliases = eval.fields();
            } else {
                // Only Project and Eval rebind an attribute id onto a new child expression. Every other node that
                // introduces attributes - FORK, MV_EXPAND, STATS, joins, ... - materializes a runtime column that
                // cannot be pushed down to Lucene, so skipping them leaves `mapped` false.
                return;
            }
            for (NamedExpression ne : aliases) {
                if (ne instanceof Alias alias && alias.toAttribute().id().equals(current.get().id())) {
                    if (alias.child() instanceof FieldAttribute fa) {
                        mapped.set(isMapped(fa));
                        breakEarly.set(true);
                    } else if (alias.child() instanceof Attribute next) {
                        current.set(next);
                    } else {
                        breakEarly.set(true);
                    }
                    return;
                }
            }
        });
        return mapped.get();
    }

    private static boolean isMapped(FieldAttribute fa) {
        // A potentially unmapped field cannot be pushed down and is matched at runtime, so it is a legitimate
        // declaration site for the values analyzer.
        return fa.isPotentiallyUnmapped() == false;
    }

    @Override
    protected Map<DataType, BuildFactory> factories() {
        return new HashMap<>(EVALUATORS);
    }

    @Override
    public DataType dataType() {
        return TEXT;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToText(source(), newChildren.get(0), options);
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToText::new, field(), options);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        return Objects.equals(options, ((ToText) obj).options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), options);
    }
}
