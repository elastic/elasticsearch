/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.compute.lucene.LuceneTopNSourceOperator;
import org.elasticsearch.compute.operator.FilterOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

/**
 * Expressions implementing this interface are asked provide an
 * Elasticsearch/Lucene query as part of the data node optimizations.
 */
public interface TranslationAware {
    /**
     * Can this instance be translated or not? Usually checks whether the
     * expression arguments are actual fields that exist in Lucene. See {@link Translatable}
     * for precisely what can be signaled from this method.
     */
    Translatable translatable(LucenePushdownPredicates pushdownPredicates);

    /**
     * Is an {@link Expression} translatable?
     */
    static TranslationAware.Translatable translatable(Expression exp, LucenePushdownPredicates lucenePushdownPredicates) {
        if (exp instanceof TranslationAware aware) {
            return aware.translatable(lucenePushdownPredicates);
        }
        return TranslationAware.Translatable.NO;
    }

    /**
     * Translates the implementing expression into a Query.
     * If during translation a child needs to be translated first, the handler needs to be used even if the child implements this
     * interface as well. This is to ensure that the child is wrapped in a SingleValueQuery if necessary.
     * <p>So use this:</p>
     * <p>{@code Query childQuery = handler.asQuery(child);}</p>
     * <p>and <b>not</b> this:</p>
     * <p>{@code Query childQuery = child.asQuery(handler);}</p>
     */
    Query asQuery(LucenePushdownPredicates pushdownPredicates, TranslatorHandler handler);

    /**
     * Subinterface for expressions that can only process single values (and null out on MVs).
     */
    interface SingleValueTranslationAware extends TranslationAware {
        /**
         * Returns the field that only supports single-value semantics.
         */
        Expression singleValueField();
    }

    /**
     * How is this expression translatable?
     */
    enum Translatable {
        /**
         * Not translatable at all. Calling {@link TranslationAware#asQuery} is an error.
         * The expression will stay in the query plan and be filtered via a {@link FilterOperator}.
         * Imagine {@code kwd == "a"} when {@code kwd} is configured without a search index.
         */
        NO(FinishedTranslatable.NO),
        /**
         * Entirely translatable into a lucene query. Calling {@link TranslationAware#asQuery}
         * will produce a query that matches all documents matching this expression and
         * <strong>only</strong> documents matching this expression. Imagine {@code kwd == "a"}
         * when {@code kwd} has a search index and doc values - which is the
         * default configuration. This will entirely remove the clause from the
         * {@code WHERE}, removing the entire {@link FilterOperator} if it's empty. Sometimes
         * this allows us to push the entire top-n operation to lucene with
         * a {@link LuceneTopNSourceOperator}.
         */
        YES(FinishedTranslatable.YES),
        /**
         * Translation requires a recheck. Calling {@link TranslationAware#asQuery} will
         * produce a query that matches all documents matching this expression but might
         * match more documents that do not match the expression. This will cause us to
         * push a query to lucene <strong>and</strong> keep the query in the query plan,
         * rechecking it via a {@link FilterOperator}. This can never push the entire
         * top-n to Lucene, but it's still quite a lot better than the full scan from
         * {@link #NO}.
         * <p>
         *     Imagine {@code kwd == "a"} where {@code kwd} has a search index but doesn't
         *     have doc values. In that case we can find candidate matches in lucene but
         *     can't tell if those docs are single-valued. If they are multivalued they'll
         *     still match the query but won't match the expression. Thus, the double-checking.
         *     <strong>Technically</strong> we could just check for single-valued-ness in
         *     this case, but it's simpler to
         * </p>
         */
        RECHECK(FinishedTranslatable.RECHECK),
        /**
         * The same as {@link #YES}, but if this expression is negated it turns into {@link #RECHECK}.
         * This comes up when pushing {@code NOT(text == "a")} to {@code text.keyword} which can
         * have ignored fields.
         */
        YES_BUT_RECHECK_NEGATED(FinishedTranslatable.YES);

        private final FinishedTranslatable finish;

        Translatable(FinishedTranslatable finish) {
            this.finish = finish;
        }

        /**
         * Translate into a {@link FinishedTranslatable} which never
         * includes {@link #YES_BUT_RECHECK_NEGATED}.
         */
        public FinishedTranslatable finish() {
            return finish;
        }

        /**
         * <strong>Essentially</strong> the {@link TranslationAware#translatable}
         * implementation for the {@link Not} expression. When you wrap an expression
         * in {@link Not} the result is <strong>mostly</strong> pushable in the same
         * way as the original expression. But there are some expressions that aren't
         * need rechecks or can't be pushed at all. This handles that.
         */
        public Translatable negate() {
            return switch (this) {
                case YES_BUT_RECHECK_NEGATED -> Translatable.RECHECK;
                case RECHECK -> Translatable.NO;
                default -> this;
            };
        }

        /**
         * Merge two {@link TranslationAware#translatable} results.
         */
        public Translatable merge(Translatable rhs) {
            return switch (this) {
                case NO -> NO;
                case YES -> switch (rhs) {
                    case NO -> NO;
                    case YES -> YES;
                    case RECHECK -> RECHECK;
                    case YES_BUT_RECHECK_NEGATED -> YES_BUT_RECHECK_NEGATED;
                };
                case RECHECK -> switch (rhs) {
                    case NO -> NO;
                    case YES, RECHECK, YES_BUT_RECHECK_NEGATED -> RECHECK;
                };
                case YES_BUT_RECHECK_NEGATED -> switch (rhs) {
                    case NO -> NO;
                    case YES, YES_BUT_RECHECK_NEGATED -> YES_BUT_RECHECK_NEGATED;
                    case RECHECK -> RECHECK;
                };
            };
        }

    }

    enum FinishedTranslatable {
        /**
         * See {@link Translatable#YES}.
         */
        YES,
        /**
         * See {@link Translatable#NO}.
         */
        NO,
        /**
         * See {@link Translatable#RECHECK}.
         */
        RECHECK;
    }
}
