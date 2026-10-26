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
import org.elasticsearch.index.mapper.TokenStreamMatching;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.AnalyzedTextExpression;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
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
import org.elasticsearch.xpack.esql.plan.logical.AliasBindings;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

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
     * terms, and each row's value is then analyzed and matched by the {@link TokenStreamMatching.Matcher} the
     * given function builds from those terms. How the terms must match (any term, consecutive phrase, ...) is the
     * only thing that differs between the full-text functions supporting runtime search.
     */
    protected ExpressionEvaluator.Factory runtimeTextEvaluator(
        ToEvaluator toEvaluator,
        Function<List<BytesRef>, TokenStreamMatching.Matcher> matcherBuilder
    ) {
        // Without options there is no query-side override, so the values analyzer covers both sides — the query
        // analyzer defaults to the values analyzer like search_analyzer does on an indexed field.
        Analyzer analyzer = resolveValuesAnalyzer(toEvaluator);
        List<BytesRef> queryTerms;
        try {
            queryTerms = TokenStreamMatching.analyzeTerms(analyzer, RuntimeSearch.CONTENT_FIELD, queryAsObject().toString());
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
            // substituted here - see mergedTextColumn.
            if (isRuntimeSearch() && plan instanceof Filter) {
                MergedTextColumn merged = mergedTextColumn(plan);
                if (merged == null) {
                    return;
                }
                if (merged.branchAnalyzers().size() > 1) {
                    List<String> analyzers = List.copyOf(merged.branchAnalyzers());
                    failures.add(
                        fail(
                            this,
                            "[{}] {} cannot search column [{}] after {}: the merged column is not index-backed, and its "
                                + "{} analyze its values with different analyzers [{}] and [{}]. Search [{}] in the {} "
                                + "instead, or declare one analyzer for the merged column with TO_TEXT({}, {\"analyzer\": ...}).",
                            functionName(),
                            functionType(),
                            merged.column().name(),
                            merged.mergeName(),
                            merged.branchesName(),
                            analyzers.get(0),
                            analyzers.get(1),
                            merged.column().name(),
                            merged.branchesName(),
                            merged.column().name()
                        )
                    );
                } else if (merged.mappedField() != null) {
                    failures.add(
                        fail(
                            this,
                            "[{}] {} cannot search column [{}] after {}: the merged column is not index-backed, so its "
                                + "values are analyzed with the [{}] analyzer rather than the analyzer mapped for [{}]. "
                                + "Search [{}] in the {} instead, or declare an analyzer for the merged column "
                                + "with TO_TEXT({}, {\"analyzer\": ...}).",
                            functionName(),
                            functionType(),
                            merged.column().name(),
                            merged.mergeName(),
                            AnalyzedTextExpression.STANDARD_ANALYZER,
                            merged.mappedField().name(),
                            // the branches know the field by its own name, which a RENAME above them may have changed
                            merged.mappedField().name(),
                            merged.branchesName(),
                            merged.column().name()
                        )
                    );
                }
            }
        };
    }

    /**
     * The merged column this runtime search reads, with what its branches fill it from.
     *
     * @param mergeName what merged the column, for a user-facing message
     * @param branchesName what that command's branches are called, for a user-facing message
     * @param mappedField a mapped {@code text} field one of the branches fills the column from, or {@code null} when
     *                    none does. It differs from {@code column} when {@code RENAME} or {@code EVAL} sits between them.
     * @param branchAnalyzers the analyzers the branches that have values analyze the column with, sorted
     */
    private record MergedTextColumn(
        Attribute column,
        String mergeName,
        String branchesName,
        @Nullable FieldAttribute mappedField,
        Set<String> branchAnalyzers
    ) {}

    /**
     * Whether this runtime search reads a {@code FORK} or {@code UNION ALL} output column, and if so what the
     * branches fill it from.
     * <p>
     * A merged column is no longer index-backed, so its values are analyzed row by row with the one analyzer declared
     * on the merged column - the standard analyzer when nothing declared one. Two things can make that silently wrong:
     * <ul>
     *     <li>A branch fills the column from a mapped {@code text} field. The default replaces the analyzer the field
     *     was mapped with, and the search answers a different question than the same search below the merge would.
     *     The declaration is the only thing that can tell the two cases apart: the merged attribute is a
     *     {@code ReferenceAttribute} of type {@code TEXT} either way, and ES|QL field types carry no analyzer.</li>
     *     <li>The branches analyze the column differently. The merged column carries the analyzer of the branch that
     *     happened to come first, and the rows of the other branches are analyzed with an analyzer they never
     *     declared. {@code FORK} rejects this when the plan is analyzed; {@code UNION ALL} still allows the column to
     *     be used in ways that do not analyze it, like {@code HIGHLIGHT}, which reads each branch's mapping.</li>
     * </ul>
     * <p>
     * Only reachable when push-down left the search above the merge. {@code PushDownFiltersIntoFork} moves a filter
     * into the branches whenever at least one of them has no pipeline breaker, and
     * {@code PushDownFilterAndLimitIntoUnionAll} always does, unless a pipeline breaker sits above the merge. Either
     * restores a search that each branch runs on its own column. {@code MV_EXPAND} substitutes the analyzer the same
     * way but is deliberately not checked: it did so before a runtime search was allowed above a pipeline breaker at
     * all, so narrowing it now would break working queries rather than restrict newly opened ground. {@code HIGHLIGHT}
     * is not checked either, for a different reason: it never searches an index, so there is no index behaviour for a
     * merged column to diverge from.
     */
    @Nullable
    private MergedTextColumn mergedTextColumn(LogicalPlan plan) {
        Expression searched = field();
        if (searched instanceof AbstractConvertFunction convertFunction) {
            // An analyzer declared right here, as in TO_TEXT(column, {"analyzer": ...}), applies to every row
            // whatever the branches declared, so there is nothing to substitute.
            if (AnalyzedTextExpression.valuesAnalyzerOf(searched) != null) {
                return null;
            }
            searched = convertFunction.field();
        }
        if (searched instanceof Attribute == false) {
            return null;
        }
        Attribute column = (Attribute) searched;
        // A keyword column is not analyzed, so it has no analyzer to lose.
        if (column.dataType() != TEXT) {
            return null;
        }
        // Checked before the bindings are collected so that the far more common merge-less plan costs one short
        // circuiting walk rather than a map of every alias in it.
        if (plan.anyMatch(p -> p instanceof MergePlan) == false) {
            return null;
        }

        AttributeMap<Expression> aliases = AliasBindings.of(plan);
        Expression resolved = aliases.resolve(column, column);
        if (resolved instanceof Attribute == false) {
            return null;
        }
        Attribute merged = (Attribute) resolved;
        MergePlan merge = findDefiningMerge(plan, merged);
        if (merge == null) {
            return null;
        }

        Holder<FieldAttribute> mappedField = new Holder<>();
        Set<String> branchAnalyzers = new TreeSet<>();
        collectBranchSources(merge, merged.name(), mappedField, branchAnalyzers);
        return merge instanceof Fork
            ? new MergedTextColumn(column, "FORK", "FORK branches", mappedField.get(), branchAnalyzers)
            : new MergedTextColumn(column, "subqueries in FROM", "subqueries", mappedField.get(), branchAnalyzers);
    }

    /**
     * Finds the {@code FORK} or subquery union in {@code plan} that combined {@code column} from its branches, or
     * returns {@code null} when {@code column} is not a merged column.
     * <p>
     * A merge gives each of its output columns a new id, and the commands above it pass the column on under that id.
     * So the merge whose output has {@code column}'s id is the one that created it. An {@code EVAL} or {@code RENAME}
     * between them binds a new id, so resolve {@code column} through {@link AliasBindings} first.
     */
    @Nullable
    private static MergePlan findDefiningMerge(LogicalPlan plan, Attribute column) {
        Holder<MergePlan> found = new Holder<>();
        plan.forEachDown(MergePlan.class, merge -> {
            if (found.get() == null && merge.output().stream().anyMatch(out -> out.id().equals(column.id()))) {
                found.set(merge);
            }
        });
        return found.get();
    }

    /**
     * Adds what the branches of {@code merge} fill its column {@code name} from to {@code mappedField} and
     * {@code branchAnalyzers}. A branch can pass on a column that a merge inside it produced, like a {@code FORK}
     * inside a subquery. That merged column hides the mapped field behind it and carries only the analyzer of its own
     * first branch, so the nested merge's branches are collected instead.
     */
    private static void collectBranchSources(
        MergePlan merge,
        String name,
        Holder<FieldAttribute> mappedField,
        Set<String> branchAnalyzers
    ) {
        // Branches are name-aligned by the time the merge resolves, so the name is what identifies the same
        // column across them. The merge gives its output new ids, so no branch column shares the merged column's id.
        for (LogicalPlan branch : merge.children()) {
            Predicate<Attribute> onlyNull = Fork.producesOnlyNull(branch);
            for (Attribute branchColumn : branch.output()) {
                if (branchColumn.name().equals(name) == false) {
                    continue;
                }
                // bound per branch, since a FORK's branches bind the same ids
                Expression source = AliasBindings.of(branch).resolve(branchColumn, branchColumn);
                MergePlan nested = source instanceof Attribute attribute ? findDefiningMerge(branch, attribute) : null;
                if (nested != null) {
                    collectBranchSources(nested, ((Attribute) source).name(), mappedField, branchAnalyzers);
                    break;
                }
                if (mappedField.get() == null && source instanceof FieldAttribute mapped && mapped.dataType() == TEXT) {
                    mappedField.set(mapped);
                }
                // A branch that does not produce the column was filled with nulls, which have nothing to analyze.
                if (onlyNull.test(branchColumn) == false) {
                    String declared = AnalyzedTextExpression.valuesAnalyzerOf(branchColumn);
                    branchAnalyzers.add(declared == null ? AnalyzedTextExpression.STANDARD_ANALYZER : declared);
                }
                break;
            }
        }
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
