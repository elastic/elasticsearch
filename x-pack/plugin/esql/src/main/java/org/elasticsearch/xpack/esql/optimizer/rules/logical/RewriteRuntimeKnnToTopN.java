/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.expression.function.vector.CosineSimilarity;
import org.elasticsearch.xpack.esql.expression.function.vector.Knn;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;

import java.util.ArrayList;
import java.util.List;

/**
 * Rewrites a {@code Filter} involving runtime-field KNN function into an exact brute-force plan
 * which can be executed entirely in the compute engine:
 * <p>
 * For an example ESQL query like,
 * <pre>
 *   WHERE knn(expr, query) [AND prefilter...]
 *   LIMIT k
 * </pre>
 *
 * The pre-optimization plan is:
 *
 * <pre>
 *  Project(outputs)
 *    Filter[KNN(expr, query) AND prefilter...]
 *      child
 * </pre>
 *
 * After optimization, KNN is rewritten to an Eval(similarity) + TopN plan applied after rest of the filters.
 *
 * <pre>
 *   Project(original outputs)
 *     TopN($$knn_similarity$$ DESC, k)
 *       Eval($$knn_similarity$$ = v_cosine(expr, query))
 *         [Filter(prefilter)]
 *           original child
 * </pre>
 *
 * A KNN is treated as a runtime search when its field argument is not an indexed
 * {@code dense_vector} FieldAttribute — for example a column produced by EVAL or
 * read from a federated data source.  In that case Lucene pushdown is impossible
 * and this rewrite provides the execution path.
 *
 * Non-KNN conjuncts in the WHERE clause become a prefilter applied before the
 * similarity computation, mirroring the prefilter semantics of the pushdown path.
 *
 * The rule fires only after {@link PushLimitToKnn} has populated {@code implicitK};
 * if that has not happened yet the rule is a no-op and defers to the next optimizer
 * iteration.  A KNN nested inside a disjunction (OR) is left untouched — only
 * top-level conjunctions in the WHERE are handled.
 */
public class RewriteRuntimeKnnToTopN extends OptimizerRules.OptimizerRule<Filter> {
    public static String TEMP_COL_NAME = "$$knn_similarity$$";

    @Override
    protected LogicalPlan rule(Filter filter) {
        List<Knn> runtimeKnns = new ArrayList<>();
        List<Expression> nonKnnConjuncts = new ArrayList<>();
        collectFromConjunction(filter.condition(), runtimeKnns, nonKnnConjuncts);

        if (runtimeKnns.isEmpty()) {
            return filter;
        }
        // Multiple runtime KNNs in one WHERE are not yet supported
        if (runtimeKnns.size() != 1) {
            return filter;
        }

        Knn knn = runtimeKnns.get(0);
        // Make sure we apply transformation only to KNN on runtime fields
        assert knn.isRuntimeSearch() : "RewriteRuntimeKnnToTopN should only be applied to runtime KNNs";

        // implicitK is populated by PushLimitToKnn; defer until it runs
        if (knn.implicitK() == null) {
            return filter;
        }

        Source source = knn.source();
        LogicalPlan child = filter.child();

        // Apply non-runtime-KNN conjuncts as a prefilter below the similarity computation
        if (nonKnnConjuncts.isEmpty() == false) {
            // TODO: check if source is correct here.
            child = new Filter(source, child, combineConjuncts(nonKnnConjuncts, source));
        }

        // Compute cosine similarity for every (pre-filtered) row
        // TODO: accept similarity metric.
        var similarityExpr = new CosineSimilarity(source, knn.field(), knn.query());
        // TODO: is the name unique?
        var simAlias = new Alias(source, TEMP_COL_NAME, similarityExpr, null, true);
        var eval = new Eval(source, child, List.of(simAlias));

        // Select the k rows with the highest similarity
        var simRef = simAlias.toAttribute();
        var order = new Order(source, simRef, Order.OrderDirection.DESC, Order.NullsPosition.LAST);
        var kLiteral = new Literal(source, knn.explicitK() != null ? knn.explicitK() : knn.implicitK(), DataType.INTEGER);
        var topN = new TopN(source, eval, List.of(order), kLiteral, false);

        // Drop the synthetic similarity column; expose the same attributes the Filter did
        return new Project(source, topN, filter.output());
    }

    /**
     * Walks a flat AND chain and partitions its leaves into runtime KNN functions
     * and everything else. A KNN nested inside an OR is ignored (treated as a non-KNN term)
     * because we cannot safely rewrite disjunctions this way.
     */
    private static void collectFromConjunction(Expression expr, List<Knn> runtimeKnns, List<Expression> others) {
        if (expr instanceof And and) {
            collectFromConjunction(and.left(), runtimeKnns, others);
            collectFromConjunction(and.right(), runtimeKnns, others);
        } else if (expr instanceof Knn knn && knn.isRuntimeSearch()) {
            runtimeKnns.add(knn);
        } else {
            others.add(expr);
        }
    }

    private static Expression combineConjuncts(List<Expression> exprs, Source source) {
        assert exprs.isEmpty() == false;
        Expression result = exprs.get(0);
        for (int i = 1; i < exprs.size(); i++) {
            result = new And(source, result, exprs.get(i));
        }
        return result;
    }
}
