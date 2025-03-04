/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator;
import org.elasticsearch.compute.lucene.LuceneQueryExpressionEvaluator.ShardConfig;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisPlanVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TypeResolutions;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;
import org.elasticsearch.xpack.esql.expression.predicate.logical.BinaryLogic;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
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

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNotNullAndFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Base class for full-text functions that use ES queries to match documents.
 * These functions needs to be pushed down to Lucene queries to be executed - there's no Evaluator for them, but depend on
 * {@link org.elasticsearch.xpack.esql.optimizer.LocalPhysicalPlanOptimizer} to rewrite them into Lucene queries.
 */
public abstract class FullTextFunction extends Function implements TranslationAware, PostAnalysisPlanVerificationAware, EvaluatorMapper {

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
        return isString(query(), sourceText(), queryOrdinal).and(isNotNullAndFoldable(query(), sourceText(), queryOrdinal));
    }

    public Expression query() {
        return query;
    }

    /**
     * Returns the resulting query as an object
     *
     * @return query expression as an object
     */
    public Object queryAsObject() {
        Object queryAsObject = query().fold(FoldContext.small() /* TODO remove me */);
        return BytesRefs.toString(queryAsObject);
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
        return Objects.hash(super.hashCode(), queryBuilder);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }

        return Objects.equals(queryBuilder, ((FullTextFunction) obj).queryBuilder);
    }

    @Override
    public boolean translatable(LucenePushdownPredicates pushdownPredicates) {
        // In isolation, full text functions are pushable to source. We check if there are no disjunctions in Or conditions
        return true;
    }

    @Override
    public Query asQuery(TranslatorHandler handler) {
        return queryBuilder != null ? new TranslationAwareExpressionQuery(source(), queryBuilder) : translate(handler);
    }

    public QueryBuilder queryBuilder() {
        return queryBuilder;
    }

    protected abstract Query translate(TranslatorHandler handler);

    public abstract Expression replaceQueryBuilder(QueryBuilder queryBuilder);

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
                Match.class,
                lp -> (lp instanceof Limit == false) && (lp instanceof Aggregate == false),
                m -> "[" + m.functionName() + "] " + m.functionType(),
                failures
            );
            checkCommandsBeforeExpression(
                plan,
                condition,
                Term.class,
                lp -> (lp instanceof Limit == false) && (lp instanceof Aggregate == false),
                m -> "[" + m.functionName() + "] " + m.functionType(),
                failures
            );
            checkFullTextFunctionsParents(condition, failures);

            boolean usesScore = plan.output()
                .stream()
                .anyMatch(attr -> attr instanceof MetadataAttribute ma && ma.name().equals(MetadataAttribute.SCORE));
            if (usesScore) {
                checkFullTextSearchDisjunctions(condition, failures);
            }
        } else {
            plan.forEachExpression(FullTextFunction.class, ftf -> {
                failures.add(fail(ftf, "[{}] {} is only supported in WHERE commands", ftf.functionName(), ftf.functionType()));
            });
        }
    }

    /**
     * Checks whether a condition contains a disjunction with a full text search.
     * If it does, check that every element of the disjunction is a full text search or combinations (AND, OR, NOT) of them.
     * If not, add a failure to the failures collection.
     *
     * @param condition        condition to check for disjunctions of full text searches
     * @param failures         failures collection to add to
     */
    private static void checkFullTextSearchDisjunctions(Expression condition, Failures failures) {
        Holder<Boolean> isInvalid = new Holder<>(false);
        condition.forEachDown(Or.class, or -> {
            if (isInvalid.get()) {
                // Exit early if we already have a failures
                return;
            }
            if (checkDisjunctionPushable(or) == false) {
                isInvalid.set(true);
                failures.add(
                    fail(
                        or,
                        "Invalid condition when using METADATA _score [{}]. Full text functions can be used in an OR condition, "
                            + "but only if just full text functions are used in the OR condition",
                        or.sourceText()
                    )
                );
            }
        });
    }

    /**
     * Checks if a disjunction is pushable from the point of view of FullTextFunctions. Either it has no FullTextFunctions or
     * all it contains are FullTextFunctions.
     *
     * @param or disjunction to check
     * @return true if the disjunction is pushable, false otherwise
     */
    private static boolean checkDisjunctionPushable(Or or) {
        boolean hasFullText = or.anyMatch(FullTextFunction.class::isInstance);
        return hasFullText == false || onlyFullTextFunctionsInExpression(or);
    }

    /**
     * Checks whether an expression contains just full text functions or negations (NOT) and combinations (AND, OR) of full text functions
     *
     * @param expression expression to check
     * @return true if all children are full text functions or negations of full text functions, false otherwise
     */
    private static boolean onlyFullTextFunctionsInExpression(Expression expression) {
        if (expression instanceof FullTextFunction) {
            return true;
        } else if (expression instanceof Not) {
            return onlyFullTextFunctionsInExpression(expression.children().get(0));
        } else if (expression instanceof BinaryLogic binaryLogic) {
            return onlyFullTextFunctionsInExpression(binaryLogic.left()) && onlyFullTextFunctionsInExpression(binaryLogic.right());
        }

        return false;
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

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        List<EsPhysicalOperationProviders.ShardContext> shardContexts = toEvaluator.shardContexts();
        ShardConfig[] shardConfigs = new ShardConfig[shardContexts.size()];
        int i = 0;
        for (EsPhysicalOperationProviders.ShardContext shardContext : shardContexts) {
            shardConfigs[i++] = new ShardConfig(shardContext.toQuery(queryBuilder()), shardContext.searcher());
        }
        return new LuceneQueryExpressionEvaluator.Factory(shardConfigs);
    }
}
