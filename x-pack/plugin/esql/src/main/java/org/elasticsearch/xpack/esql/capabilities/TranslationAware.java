/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.util.Check;
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

    Query asQuery(TranslatorHandler translatorHandler);

    static TranslationAware checkIsTranslationAware(Expression expression) {
        Check.isTrue(
            expression instanceof TranslationAware,
            "Expected a TranslationAware but received [{}] of type [{}]",
            expression,
            expression.getClass()
        );
        return (TranslationAware) expression;
    }
}
