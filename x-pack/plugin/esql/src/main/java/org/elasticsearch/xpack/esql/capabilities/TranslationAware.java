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
    boolean translatable(LucenePushdownPredicates pushdownPredicates);

    /**
     * Translates the implementing expression into a Query.
     * If during translation a child needs to be translated first, the handler needs to be used even if the child implements this
     * interface as well. This is to ensure that the child is wrapped in a SingleValueQuery if necessary.
     * <p>So use this:</p>
     * <p>{@code Query childQuery = handler.asQuery(child);}</p>
     * <p>and <b>not</b> this:</p>
     * <p>{@code Query childQuery = child.asQuery(handler);}</p>
     */
    Query asQuery(TranslatorHandler handler);

    /**
     * Subinterface for expressions that can only process single values (and null out on MVs).
     */
    interface SingleValueTranslationAware extends TranslationAware {
        /**
         * Returns the field that only supports single-value semantics.
         */
        Expression singleValueField();
    }
}
