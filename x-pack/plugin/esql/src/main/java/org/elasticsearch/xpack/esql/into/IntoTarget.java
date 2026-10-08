/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.into;

import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.parser.EsqlConfig;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Into;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Optional;

/**
 * A query that ends in {@code INTO}, split into the part that produces rows and where they are written.
 *
 * @param producer    the query up to but not including {@code INTO}
 * @param destination where the rows are written
 */
public record IntoTarget(String producer, String destination) {

    /** Returns the split when the query ends in {@code INTO}, or empty when it is an ordinary read. */
    public static Optional<IntoTarget> split(String query) {
        if (query == null || query.toLowerCase(java.util.Locale.ROOT).contains("into") == false) {
            // Cheap reject so ordinary queries are not parsed twice.
            return Optional.empty();
        }
        final LogicalPlan plan;
        try {
            plan = new EsqlParser(new EsqlConfig(new EsqlFunctionRegistry())).parseQuery(query);
        } catch (Exception e) {
            // Let the normal execution path produce the parse error, with its own line and column information.
            return Optional.empty();
        }
        if ((plan instanceof Into) == false) {
            return Optional.empty();
        }
        final Into into = (Into) plan;
        final String intoText = into.source().text();
        final int start = query.lastIndexOf(intoText);
        if (start < 0) {
            return Optional.empty();
        }
        String producer = query.substring(0, start).stripTrailing();
        if (producer.endsWith("|")) {
            producer = producer.substring(0, producer.length() - 1).stripTrailing();
        }
        if (producer.isEmpty()) {
            throw new IllegalArgumentException("INTO needs a query before it");
        }
        Into.verifyProducer(into.child(), into.destination());
        return Optional.of(new IntoTarget(producer, into.destination()));
    }
}
