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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationPlanVerificationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Foldables;
import org.elasticsearch.xpack.esql.expression.function.Options;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isType;
import static org.elasticsearch.xpack.esql.core.type.DataType.DATE_NANOS;
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
        // TODO: use the analyzer specified in the options, for now we use the standard analyzer.
        Analyzer analyzer = new StandardAnalyzer();
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
        return (plan, failures) -> {
            super.postAnalysisPlanVerification().accept(plan, failures);
            fieldVerifier(plan, this, field, failures);
        };
    }

    @Override
    public BiConsumer<LogicalPlan, Failures> postOptimizationPlanVerification() {
        // check plan again after predicates are pushed down into subqueries
        return (plan, failures) -> {
            super.postOptimizationPlanVerification().accept(plan, failures);
            fieldVerifier(plan, this, field, failures);
        };
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
