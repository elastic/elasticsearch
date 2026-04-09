/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.compute.lucene.LuceneQueryEvaluator.ShardConfig;
import org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator;
import org.elasticsearch.compute.lucene.LuceneQueryScoreEvaluator;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.compute.operator.ScoreOperator;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.PostOptimizationVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.RewriteableAware;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.AbstractConvertFunction;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.EsqlBinaryComparison;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.querydsl.query.TranslationAwareExpressionQuery;
import org.elasticsearch.xpack.esql.score.ExpressionScoreMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNull;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPostOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.TypeResolutionValidator.forPreOptimizationValidation;
import static org.elasticsearch.xpack.esql.expression.Foldables.resolveTypeQuery;

/**
 * Base class for full-text functions that use ES queries to match documents.
 * These functions needs to be pushed down to Lucene queries to be executed - there’s no Evaluator for them, but depend on
 * {@link org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer} to rewrite them into Lucene queries.
 */
public abstract class FullTextFunction extends Function
    implements
        TranslationAware,
        PostAnalysisPlanVerificationAware,
        EvaluatorMapper,
        ExpressionScoreMapper,
        PostOptimizationVerificationAware,
        RewriteableAware {

    private final Expression query;
    private final QueryBuilder queryBuilder;

    protected FullTextFunction(Source source, Expression query, List<Expression> children, QueryBuilder queryBuilder) {
        super(source, children);
        this.query = query;
        this.queryBuilder = queryBuilder;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    protected final TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return resolveParams();
    }

    /**
     * Resolves the type for the function parameters, as part of the type resolution for the function
     *
     * @return type resolution for the function parameters
     */
    protected TypeResolution resolveParams() {
        return resolveQuery(DEFAULT);
    }

    /**
     * Resolves the type for the query parameter, as part of the type resolution for the function
     *
     * @return type resolution for the query parameter
     */
    protected TypeResolution resolveQuery(TypeResolutions.ParamOrdinal queryOrdinal) {
        TypeResolution result = isString(query(), sourceText(), queryOrdinal).and(isNotNull(query(), sourceText(), queryOrdinal));
        if (result.unresolved()) {
            return result;
        }
        result = resolveTypeQuery(query(), sourceText(), forPreOptimizationValidation(query()));
        if (result.equals(TypeResolution.TYPE_RESOLVED) == false) {
            return result;
        }
        return TypeResolution.TYPE_RESOLVED;
    }

    public Expression query() {
        return query;
    }

    @Override
    public Nullability nullable() {
        return Nullability.FALSE;
    }

    /**
     * Used to differentiate error messages between functions and operators
     *
     * @return function type for error messages
     */
    public String functionType() {
        return "function";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), query, System.identityHashCode(queryBuilder));
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }

        // Compare query builders using identity because that's how they are compared during query rewriting
        FullTextFunction other = (FullTextFunction) obj;
        return queryBuilder == other.queryBuilder && Objects.equals(query, other.query);
    }

    @Override
    public Translatable translatable(LucenePushdownPredicates pushdownPredicates) {
        return Translatable.YES;
    }

    @Override
    public Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler) {
        return queryBuilder != null ? new TranslationAwareExpressionQuery(source(), queryBuilder) : translate(pushdownPredicates, handler);
    }

    @Override
    public QueryBuilder queryBuilder() {
        return queryBuilder;
    }

    protected abstract Query translate(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler);

    @Override
    public BiConsumer<LogicalPlan, Failures> postAnalysisPlanVerification() {
        return FullTextFunction::checkFullTextQueryFunctions;
    }

    /**
     * Checks full text query functions for invalid usage.
     *
     * @param plan root plan to check
     * @param failures failures found
     */
    private static void checkFullTextQueryFunctions(LogicalPlan plan, Failures failures) {
        if (plan instanceof Filter f) {
            Expression condition = f.condition();

            if (condition instanceof Score) {
                failures.add(fail(condition, "[SCORE] function can't be used in WHERE"));
            }

            List.of(QueryString.class, Kql.class).forEach(functionClass -> {
                // Check for limitations of QSTR and KQL function.
                checkCommandsBeforeExpression(
                    plan,
                    condition,
                    functionClass,
                    lp -> (lp instanceof Filter || lp instanceof OrderBy || lp instanceof EsRelation),
                    fullTextFunction -> "[" + fullTextFunction.functionName() + "] " + fullTextFunction.functionType(),
                    failures
                );
            });

            checkCommandsBeforeExpression(
                plan,
                condition,
                FullTextFunction.class,
                lp -> (lp instanceof Limit == false) && (lp instanceof Aggregate == false),
                m -> "[" + m.functionName() + "] " + m.functionType(),
                failures
            );
            checkFullTextFunctionsParents(condition, failures);
        } else if (plan instanceof Aggregate agg) {
            checkFullTextFunctionsInAggs(agg, failures);
        } else {
            List<FullTextFunction> scoredFTFs = new ArrayList<>();
            plan.forEachExpression(Score.class, scoreFunction -> {
                checkScoreFunction(plan, failures, scoreFunction);
                plan.forEachExpression(FullTextFunction.class, scoredFTFs::add);
            });
            plan.forEachExpression(FullTextFunction.class, ftf -> {
                if (scoredFTFs.remove(ftf) == false) {
                    failures.add(
                        fail(
                            ftf,
                            "[{}] {} is only supported in WHERE and STATS commands"
                                + (EsqlCapabilities.Cap.SCORE_FUNCTION.isEnabled() ? ", or in EVAL within score(.) function" : ""),
                            ftf.functionName(),
                            ftf.functionType()
                        )
                    );
                }
            });
        }
    }

    private static void checkScoreFunction(LogicalPlan plan, Failures failures, Score scoreFunction) {
        checkCommandsBeforeExpression(
            plan,
            scoreFunction.canonical(),
            Score.class,
            lp -> (lp instanceof Limit == false) && (lp instanceof Aggregate == false),
            m -> "[" + m.functionName() + "] function",
            failures
        );
    }

    private static void checkFullTextFunctionsInAggs(Aggregate agg, Failures failures) {
        agg.groupings().forEach(exp -> {
            exp.forEachDown(e -> {
                if (e instanceof FullTextFunction ftf) {
                    failures.add(
                        fail(ftf, "[{}] {} is only supported in WHERE and STATS commands", ftf.functionName(), ftf.functionType())
                    );
                }
            });
        });
    }

    /**
     * Checks all commands that exist before a specific type satisfy conditions.
     *
     * @param plan plan that contains the condition
     * @param condition condition to check
     * @param typeToken type to check for. When a type is found in the condition, all plans before the root plan are checked
     * @param commandCheck check to perform on each command that precedes the plan that contains the typeToken
     * @param typeErrorMsgProvider provider for the type name in the error message
     * @param failures failures to add errors to
     * @param <E> class of the type to look for
     */
    private static <E extends Expression> void checkCommandsBeforeExpression(
        LogicalPlan plan,
        Expression condition,
        Class<E> typeToken,
        Predicate<LogicalPlan> commandCheck,
        java.util.function.Function<E, String> typeErrorMsgProvider,
        Failures failures
    ) {
        condition.forEachDown(typeToken, exp -> {
            plan.forEachDown(LogicalPlan.class, lp -> {
                if (commandCheck.test(lp) == false) {
                    failures.add(
                        fail(
                            plan,
                            "{} cannot be used after {}",
                            typeErrorMsgProvider.apply(exp),
                            lp.sourceText().split(" ")[0].toUpperCase(Locale.ROOT)
                        )
                    );
                }
            });
        });
    }

    /**
     * Checks parents of a full text function to ensure they are allowed
     * @param condition condition that contains the full text function
     * @param failures failures to add errors to
     */
    private static void checkFullTextFunctionsParents(Expression condition, Failures failures) {
        forEachFullTextFunctionParent(condition, (ftf, parent) -> {
            if ((parent instanceof FullTextFunction == false)
                && (parent instanceof BinaryLogic == false)
                && (parent instanceof EsqlBinaryComparison == false)
                && (parent instanceof Not == false)) {
                failures.add(
                    fail(
                        condition,
                        "Invalid condition [{}]. [{}] {} can't be used with {}",
                        condition.sourceText(),
                        ftf.functionName(),
                        ftf.functionType(),
                        ((Function) parent).functionName()
                    )
                );
            }
        });
    }

    /**
     * Executes the action on every parent of a FullTextFunction in the condition if it is found
     *
     * @param action the action to execute for each parent of a FullTextFunction
     */
    private static FullTextFunction forEachFullTextFunctionParent(Expression condition, BiConsumer<FullTextFunction, Expression> action) {
        if (condition instanceof FullTextFunction ftf) {
            return ftf;
        }
        for (Expression child : condition.children()) {
            FullTextFunction foundMatchingChild = forEachFullTextFunctionParent(child, action);
            if (foundMatchingChild != null) {
                action.accept(foundMatchingChild, condition);
                return foundMatchingChild;
            }
        }
        return null;
    }

    protected void fieldVerifier(LogicalPlan plan, FullTextFunction function, Expression field, Failures failures) {
        // Accept null as a field
        if (Expressions.isGuaranteedNull(field)) {
            return;
        }
        var fieldAttribute = fieldAsFieldAttribute(field);
        if (fieldAttribute == null) {
            plan.forEachExpression(function.getClass(), m -> {
                if (function.children().contains(field)) {
                    failures.add(
                        fail(
                            field,
                            "[{}] {} cannot operate on [{}], which is not a field from an index mapping",
                            m.functionName(),
                            m.functionType(),
                            field.sourceText()
                        )
                    );
                }
            });
        } else {
            // Traverse the plan to find the EsRelation outputting the field
            plan.forEachDown(p -> {
                if (p instanceof EsRelation esRelation && esRelation.indexMode() != IndexMode.STANDARD) {
                    // Check if this EsRelation supplies the field
                    if (esRelation.outputSet().contains(fieldAttribute)) {
                        failures.add(
                            fail(
                                field,
                                "[{}] {} cannot operate on [{}], supplied by an index [{}] in non-STANDARD mode [{}]",
                                function.functionName(),
                                function.functionType(),
                                field.sourceText(),
                                esRelation.indexPattern(),
                                esRelation.indexMode()
                            )
                        );
                    }
                }
            });
        }
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<EsPhysicalOperationProviders.ShardContext> shardContexts = toEvaluator.shardContexts();
        ShardConfig[] shardConfigs = new ShardConfig[shardContexts.size()];
        int i = 0;
        for (EsPhysicalOperationProviders.ShardContext shardContext : shardContexts) {
            shardConfigs[i++] = new ShardConfig(shardContext.toQuery(evaluatorQueryBuilder()), shardContext.searcher());
        }
        return new LuceneQueryExpressionEvaluator.Factory(shardConfigs);
    }

    /**
     * Returns the query builder to be used when the function cannot be pushed down to Lucene, but uses a
     * {@link org.elasticsearch.compute.lucene.LuceneQueryEvaluator} instead
     *
     * @return the query builder to be used in the {@link org.elasticsearch.compute.lucene.LuceneQueryEvaluator}
     */
    protected QueryBuilder evaluatorQueryBuilder() {
        // Use the same query builder as for the translation by default
        return queryBuilder();
    }

    @Override
    public ScoreOperator.ExpressionScorer.Factory toScorer(ToScorer toScorer) {
        List<EsPhysicalOperationProviders.ShardContext> shardContexts = toScorer.shardContexts();
        ShardConfig[] shardConfigs = new ShardConfig[shardContexts.size()];
        int i = 0;
        for (EsPhysicalOperationProviders.ShardContext shardContext : shardContexts) {
            shardConfigs[i++] = new ShardConfig(shardContext.toQuery(evaluatorQueryBuilder()), shardContext.searcher());
        }
        return new LuceneQueryScoreEvaluator.Factory(shardConfigs);
    }

    // TODO: this should likely be replaced by calls to FieldAttribute#fieldName; the MultiTypeEsField case looks
    // wrong if `fieldAttribute` is a subfield, e.g. `parent.child` - multiTypeEsField#getName will just return `child`.
    protected String getNameFromFieldAttribute(FieldAttribute fieldAttribute) {
        String fieldName = fieldAttribute.name();
        if (fieldAttribute.field() instanceof MultiTypeEsField multiTypeEsField) {
            // If we have multiple field types, we allow the query to be done, but getting the underlying field name
            fieldName = multiTypeEsField.getName();
        }
        return fieldName;
    }

    protected FieldAttribute fieldAsFieldAttribute(Expression field) {
        Expression fieldExpression = field;
        // Field may be converted to other data type (field_name :: data_type), so we need to check the original field
        if (fieldExpression instanceof AbstractConvertFunction convertFunction) {
            fieldExpression = convertFunction.field();
        }
        return fieldExpression instanceof FieldAttribute fieldAttribute ? fieldAttribute : null;
    }

    @Override
    public void postOptimizationVerification(Failures failures) {
        resolveTypeQuery(query(), sourceText(), forPostOptimizationValidation(query(), failures));
    }
}
