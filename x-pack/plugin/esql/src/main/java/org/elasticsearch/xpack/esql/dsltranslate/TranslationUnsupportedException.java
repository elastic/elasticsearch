/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

/**
 * Thrown by {@link QueryDslTranslator} when a Query DSL construct is outside the supported subset and has no faithful
 * ES|QL equivalent. Translation is fail-closed: the <em>consumer</em> turns this into a query error rather than guessing
 * — {@link RequestFilterRewriter} catches it and maps it to a 400 naming the construct. The translator only reports which
 * construct it could not translate; the consumer decides the error shape.
 */
public class TranslationUnsupportedException extends RuntimeException {

    private final String construct;
    private final String reason;

    public TranslationUnsupportedException(String construct) {
        this(construct, null);
    }

    /**
     * @param reason why this construct could not be translated HERE, when the answer is not "ES|QL cannot express it".
     *               A version gate is the case that matters: the construct is perfectly supported, just not by every
     *               node this query targets, and an operator told otherwise goes looking for a capability that is
     *               already there. Null means the construct has no translation at all.
     */
    public TranslationUnsupportedException(String construct, String reason) {
        super("Query DSL construct [" + construct + "] has no ES|QL translation");
        this.construct = construct;
        this.reason = reason;
    }

    /** Why this construct failed here, or {@code null} when it simply has no translation. */
    public String reason() {
        return reason;
    }

    /** The DSL construct name (e.g. {@code wildcard}, {@code geo_bounding_box}) that could not be translated. */
    public String construct() {
        return construct;
    }
}
