/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.xpack.esql.core.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;

/**
 * Expressions can implement this interface to control how they would be translated and pushed down as Lucene queries.
 * When an expression implements {@link TranslationAware}, we call {@link #asQuery(TranslatorHandler)} to get the
 * {@link Query} translation, instead of relying on the registered translators from EsqlExpressionTranslators.
 */
public interface TranslationAware {
    Query asQuery(TranslatorHandler translatorHandler);
}
