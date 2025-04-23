/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;

/**
 * Expressions implementing this interface can get called on data nodes to provide an Elasticsearch/Lucene query.
 */
public interface TranslationAware {
    /**
     * Indicates whether the expression can be translated or not.
     * Usually checks whether the expression arguments are actual fields that exist in Lucene.
     */
    Translatable translatable(LucenePushdownPredicates pushdownPredicates);

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

    enum Translatable {
        NO(FinishedTranslatable.NO),
        YES(FinishedTranslatable.YES),
        RECHECK(FinishedTranslatable.RECHECK),
        YES_BUT_RECHECK_NEGATED(FinishedTranslatable.YES);

        private final FinishedTranslatable finish;

        Translatable(FinishedTranslatable finish) {
            this.finish = finish;
        }

        public FinishedTranslatable finish() {
            return finish;
        }

        public Translatable negate() {
            if (this == YES_BUT_RECHECK_NEGATED) {
                return RECHECK;
            }
            return this;
        }

        public Translatable and(Translatable rhs) {
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
        YES,
        NO,
        RECHECK;
    }
}
