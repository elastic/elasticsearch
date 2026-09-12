/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.resolveTypeQuery;

/**
 * Base class for full-text functions that operate on a single field.
 * This class extracts common functionality from Match and MatchPhrase including:
 * - Field and options management
 * - Type resolution for field and query parameters
 * - Query value conversion
 * - Field verification
 * - Serialization patterns
 */
public abstract class SingleFieldFullTextFunction extends FullTextFunction
    implements
        PostAnalysisPlanVerificationAware,
        PostOptimizationPlanVerificationAware {

    protected final Expression field;
    private final Expression options;

    /**
     * These functions take the thing to search as an argument, so it can be an expression rather than a field.
     * {@code KNN} narrows this to what its configuration allows.
     */
    @Override
    public boolean supportsRuntimeSearch() {
        return true;
    }

    protected SingleFieldFullTextFunction(
        Source source,
        Expression field,
        Expression query,
        Expression options,
        List<Expression> children,
        QueryBuilder queryBuilder
    ) {
        super(source, query, children, queryBuilder);
        this.field = field;
        this.options = options;
    }

    public Expression field() {
        return field;
    }

    public Expression options() {
        return options;
    }

    @Override
    protected TypeResolution resolveParams() {
        return resolveField().and(resolveQuery()).and(resolveOptions());
    }

    /**
     * Resolves and validates the field parameter type.
     */
    protected TypeResolution resolveField() {
        return isType(field, getFieldDataTypes()::contains, sourceText(), TypeResolutions.ParamOrdinal.FIRST, expectedFieldTypesString());
    }

    /**
     * Resolves and validates the query parameter type.
     */
    protected TypeResolution resolveQuery() {
        TypeResolution result = isType(
            query(),
            getQueryDataTypes()::contains,
            sourceText(),
            TypeResolutions.ParamOrdinal.SECOND,
            expectedQueryTypesString()
        ).and(isNotNull(query(), sourceText(), TypeResolutions.ParamOrdinal.SECOND));
        if (result.unresolved()) {
            return result;
        }
        return resolveTypeQuery(query(), sourceText(), forPreOptimizationValidation(query()));
    }

    /**
     * Resolves and validates the options parameter.
     * Subclasses can override to add custom validation.
     */
    protected TypeResolution resolveOptions() {
        // Options are optional, so only validate if provided
        if (options() == null) {
            return TypeResolution.TYPE_RESOLVED;
        }
        return Options.resolve(options(), source(), TypeResolutions.ParamOrdinal.THIRD, getAllowedOptions());
    }

    /**
     * Converts the query expression to an Object suitable for the Lucene query.
     * Handles common conversions for BytesRef, UNSIGNED_LONG, DATETIME, and DATE_NANOS.
     */
    protected Object queryAsObject() {
        Object queryAsObject = Foldables.queryAsObject(query(), sourceText());

        // Convert BytesRef to string for string-based values
        if (queryAsObject instanceof BytesRef bytesRef) {
            return switch (query().dataType()) {
                case IP -> EsqlDataTypeConverter.ipToString(bytesRef);
                case VERSION -> EsqlDataTypeConverter.versionToString(bytesRef);
                default -> bytesRef.utf8ToString();
            };
        }

        // Converts specific types to the correct type for the query
        if (query().dataType() == DataType.UNSIGNED_LONG) {
            return org.elasticsearch.xpack.esql.core.util.NumericUtils.unsignedLongAsBigInteger((Long) queryAsObject);
        } else if (query().dataType() == DataType.DATETIME && queryAsObject instanceof Long) {
            // When casting to date and datetime, we get a long back. But Match/MatchPhrase query needs a date string
            return EsqlDataTypeConverter.dateTimeToString((Long) queryAsObject);
        } else if (query().dataType() == DATE_NANOS && queryAsObject instanceof Long) {
            return EsqlDataTypeConverter.nanoTimeToString((Long) queryAsObject);
        }

        return queryAsObject;
    }

    /**
     * Returns the field as a FieldAttribute for use in query translation
     */
    protected FieldAttribute fieldAsFieldAttribute() {
        return fieldAsFieldAttribute(field);
    }

    /**
     * Builds the runtime-search evaluator for a {@code text} field: the query string is analyzed once into its
     * terms, and each row's value is then analyzed and matched by the {@link RuntimeSearch.TokenStreamMatcher} the
     * given function builds from those terms. How the terms must match (any term, consecutive phrase, ...) is the
     * only thing that differs between the full-text functions supporting runtime search.
     */
    protected ExpressionEvaluator.Factory runtimeTextEvaluator(
        ToEvaluator toEvaluator,
        Function<List<BytesRef>, RuntimeSearch.TokenStreamMatcher> matcherBuilder
    ) {
        // Without options there is no query-side override, so the values analyzer covers both sides — the query
        // analyzer defaults to the values analyzer like search_analyzer does on an indexed field.
        Analyzer analyzer = resolveValuesAnalyzer(toEvaluator);
        List<BytesRef> queryTerms;
        try {
            queryTerms = RuntimeSearch.analyzeTerms(analyzer, queryAsObject().toString());
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to tokenize query string: " + e.getMessage(), e);
        }
        // TODO: use the `zero_terms_query` option, for now we use `none`.
        if (queryTerms.isEmpty()) {
            return ConstantEvaluators.CONSTANT_FALSE_FACTORY;
        }

        return new RuntimeSearchTextEvaluator.Factory(
            source(),
            toEvaluator.apply(field()),
            matcherBuilder.apply(queryTerms),
            analyzer,
            context -> new BytesRef()
        );
    }

    /**
     * The values analyzer declared for this function's field — the role the mapping's {@code analyzer} plays for an
     * indexed text field — or {@code null} when none was declared. Runtime text columns declare it through
     * {@code TO_TEXT}'s {@code analyzer} option; references produced by {@code EVAL}/{@code RENAME} carry the
     * declaration as attribute metadata. {@link AnalyzedTextExpression} covers both forms.
     */
    @Nullable
    protected String valuesAnalyzerName() {
        return AnalyzedTextExpression.valuesAnalyzerOf(field());
    }

    /** The declared values analyzer resolved through the registry, or the standard analyzer when none was declared. */
    protected Analyzer resolveValuesAnalyzer(EvaluatorMapper.ToEvaluator toEvaluator) {
        String name = valuesAnalyzerName();
        return name == null ? new StandardAnalyzer() : RuntimeSearch.resolveNamedAnalyzer(name, toEvaluator);
    }

    @Override
    public boolean foldable() {
        // The function is foldable if the field is guaranteed to be null, due to the field not being present in the mapping
        return Expressions.isGuaranteedNull(field());
    }

    @Override
    public Object fold(FoldContext ctx) {
        // We only fold when the field is null (it's not present in the mapping), so we return null
        return null;
    }

    @Override
    public Nullability nullable() {
        return field().nullable();
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return postAnalysisPlanVerification(null);
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification(AnalysisRegistry analysisRegistry) {
        return (plan, failures) -> {
            super.postAnalysisPlanVerification().accept(plan, failures);
            fieldVerifier(plan, this, field, analysisRegistry, failures);
        };
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        // Check plan again after predicates are pushed down into subqueries. No analysis registry is available at
        // this point, so registry-backed checks (e.g. analyzer-name validation) only run in the post-analysis pass;
        // registry inputs cannot change during optimization, so skipping them here is safe.
        return (plan, failures) -> {
            super.postOptimizationPlanVerification().accept(plan, failures);
            fieldVerifier(plan, this, field, null, failures);
            // Only a search predicate is checked. HIGHLIGHT also holds full-text functions, but it analyzes values
            // row by row through a MemoryIndex whether or not a FORK precedes it, so nothing about it is silently
            // substituted here - see forkColumnBackedByMappedTextField.
            if (isRuntimeSearch() && plan instanceof Filter) {
                ForkTextColumn forkColumn = forkColumnBackedByMappedTextField(plan);
                if (forkColumn != null) {
                    failures.add(
                        fail(
                            this,
                            "[{}] {} cannot search column [{}] after FORK: the merged column is not index-backed, so its "
                                + "values are analyzed with the [{}] analyzer rather than the analyzer mapped for [{}]. "
                                + "Search [{}] in the FORK branches instead, or declare an analyzer for the merged column "
                                + "with TO_TEXT({}, {\"analyzer\": ...}).",
                            functionName(),
                            functionType(),
                            forkColumn.column().name(),
                            AnalyzedTextExpression.STANDARD_ANALYZER,
                            forkColumn.mappedField().name(),
                            // the branches know the field by its own name, which a RENAME above them may have changed
                            forkColumn.mappedField().name(),
                            forkColumn.column().name()
                        )
                    );
                }
            }
        };
    }

    /**
     * The column this runtime search reads, paired with the mapped {@code text} field a {@code FORK} branch fills
     * it from. The two differ when {@code RENAME} or {@code EVAL} sits between them.
     */
    private record ForkTextColumn(Attribute column, FieldAttribute mappedField) {}

    /**
     * Whether this runtime search reads a {@code FORK} output column that at least one branch fills from a mapped
     * {@code text} field, and if so which field.
     * <p>
     * A {@code FORK} output column is merged from every branch, so it is no longer index-backed and its values are
     * analyzed row by row with the analyzer declared on the column - the standard analyzer when nothing declared
     * one. When those values come from a mapped {@code text} field, that default silently replaces the analyzer the
     * field was mapped with, and the search answers a different question than the same search below {@code FORK}
     * would. The declaration is the only thing that can tell the two cases apart: the merged attribute is a
     * {@code ReferenceAttribute} of type {@code TEXT} either way, and ES|QL field types carry no analyzer.
     * <p>
     * Only reachable when push-down left the search above the merge. {@code PushDownFiltersIntoFork} moves a filter
     * into the branches whenever at least one of them has no pipeline breaker, which restores an index-backed search
     * and makes {@link #isRuntimeSearch()} false. {@code MV_EXPAND} substitutes the analyzer the same way but is
     * deliberately not checked: it did so before a runtime search was allowed above a pipeline breaker at all, so
     * narrowing it now would break working queries rather than restrict newly opened ground. {@code HIGHLIGHT} is
     * not checked either, for a different reason: it never searches an index, so there is no index behaviour for a
     * merged column to diverge from.
     */
    @Nullable
    private ForkTextColumn forkColumnBackedByMappedTextField(LogicalPlan plan) {
        Expression searched = field();
        if (searched instanceof AbstractConvertFunction convertFunction) {
            searched = convertFunction.field();
        }
        if (searched instanceof Attribute == false) {
            return null;
        }
        Attribute column = (Attribute) searched;
        // A keyword column is not analyzed, so it has no mapping analyzer to lose. A declared values analyzer - which
        // Alias propagates along a chain of EVAL/RENAME - means the query already says how to analyze the values.
        if (column.dataType() != TEXT || AnalyzedTextExpression.valuesAnalyzerOf(column) != null) {
            return null;
        }
        // Checked before the bindings are collected so that the far more common FORK-less plan costs one short
        // circuiting walk rather than a map of every alias in it.
        if (plan.anyMatch(p -> p instanceof Fork) == false) {
            return null;
        }

        Map<NameId, Expression> aliases = aliasBindings(plan);
        Expression resolved = resolveThroughAliases(aliases, column);
        if (resolved instanceof Attribute == false) {
            return null;
        }
        Attribute merged = (Attribute) resolved;

        Holder<FieldAttribute> mappedField = new Holder<>();
        plan.forEachDown(Fork.class, fork -> {
            if (mappedField.get() != null || fork.output().stream().noneMatch(out -> out.id().equals(merged.id()))) {
                return;
            }
            // Branches are name-aligned by the time the merge resolves, so the name is what identifies the same
            // column across them; only the branch that wins the merge shares the output attribute's id.
            for (LogicalPlan branch : fork.children()) {
                for (Attribute branchColumn : branch.output()) {
                    if (branchColumn.name().equals(merged.name())
                        && resolveThroughAliases(aliases, branchColumn) instanceof FieldAttribute mapped
                        && mapped.dataType() == TEXT) {
                        mappedField.set(mapped);
                        return;
                    }
                }
            }
        });
        return mappedField.get() == null ? null : new ForkTextColumn(column, mappedField.get());
    }

    /**
     * Every {@code EVAL} and {@code RENAME} binding in {@code plan}, keyed by the id of the attribute it defines, so
     * that a column can be followed back to whatever produces it. Ids are unique across a plan, so a single map
     * covers both the columns above a {@code FORK} and those inside its branches.
     */
    private static Map<NameId, Expression> aliasBindings(LogicalPlan plan) {
        Map<NameId, Expression> bindings = new HashMap<>();
        plan.forEachDown(p -> {
            if (p instanceof Eval eval) {
                for (Alias alias : eval.fields()) {
                    bindings.put(alias.id(), alias.child());
                }
            } else if (p instanceof Project project) {
                for (NamedExpression projection : project.projections()) {
                    if (projection instanceof Alias alias) {
                        bindings.put(alias.id(), alias.child());
                    }
                }
            }
        });
        return bindings;
    }

    private static Expression resolveThroughAliases(Map<NameId, Expression> aliases, Expression expression) {
        Expression current = expression;
        // A binding never defines the attribute it resolves to, so no chain can be longer than the map itself.
        for (int hops = aliases.size(); hops > 0 && current instanceof Attribute attribute; hops--) {
            Expression bound = aliases.get(attribute.id());
            if (bound == null) {
                break;
            }
            current = bound;
        }
        return current;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SingleFieldFullTextFunction that = (SingleFieldFullTextFunction) o;

        // Compare query builders using identity because that's how they are compared during query rewriting
        return Objects.equals(field(), that.field())
            && Objects.equals(query(), that.query())
            && queryBuilder() == that.queryBuilder()
            && Objects.equals(options, that.options());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field(), query(), System.identityHashCode(queryBuilder()), options);
    }

    /**
     * Returns the set of allowed data types for the field parameter.
     * Each subclass defines which field types it supports.
     */
    protected abstract Set<DataType> getFieldDataTypes();

    /**
     * Returns the set of allowed data types for the query parameter.
     * Each subclass defines which query types it supports.
     */
    protected abstract Set<DataType> getQueryDataTypes();

    /**
     * Returns the allowed options map for this function.
     * Keys are option names, values are the expected data types.
     */
    protected abstract Map<String, DataType> getAllowedOptions();

    /** Resolves the query and values analyzers and delegates to {@link RuntimeSearch#textEvaluatorForQuery}. */
    protected ExpressionEvaluator.Factory textEvaluatorForQueryWithOptions(
        Query query,
        Map<String, Object> opts,
        EvaluatorMapper.ToEvaluator toEvaluator
    ) {
        NamedAnalyzer valuesAnalyzer = RuntimeSearch.resolveNamedAnalyzer(valuesAnalyzerName(), toEvaluator);
        NamedAnalyzer queryAnalyzer = RuntimeSearch.resolveNamedAnalyzer(opts, toEvaluator);
        return RuntimeSearch.textEvaluatorForQuery(
            source(),
            toEvaluator.apply(field()),
            query,
            // the analyzer option overrides the query side only, defaulting to the values analyzer
            queryAnalyzer == null ? valuesAnalyzer : queryAnalyzer,
            valuesAnalyzer
        );
    }

    /** Resolves the query and values analyzers and delegates to {@link RuntimeSearch#textScoreEvaluatorForQuery}. */
    protected ExpressionEvaluator.Factory textScoreEvaluatorForQueryWithOptions(
        Query query,
        Map<String, Object> opts,
        EvaluatorMapper.ToEvaluator toEvaluator
    ) {
        NamedAnalyzer valuesAnalyzer = RuntimeSearch.resolveNamedAnalyzer(valuesAnalyzerName(), toEvaluator);
        NamedAnalyzer queryAnalyzer = RuntimeSearch.resolveNamedAnalyzer(opts, toEvaluator);
        return RuntimeSearch.textScoreEvaluatorForQuery(
            source(),
            toEvaluator.apply(field()),
            query,
            queryAnalyzer == null ? valuesAnalyzer : queryAnalyzer,
            valuesAnalyzer
        );
    }

    /**
     * Returns a human-readable string listing the expected field types.
     * Used in error messages.
     */
    protected String expectedFieldTypesString() {
        return expectedTypesAsString(getFieldDataTypes());
    }

    /**
     * Returns a human-readable string listing the expected query types.
     * Used in error messages.
     */
    protected String expectedQueryTypesString() {
        return expectedTypesAsString(getQueryDataTypes());
    }

    static String expectedTypesAsString(Set<DataType> dataTypes) {
        return String.join(", ", dataTypes.stream().map(dt -> dt.name().toLowerCase(Locale.ROOT)).sorted().toList());
    }
}
