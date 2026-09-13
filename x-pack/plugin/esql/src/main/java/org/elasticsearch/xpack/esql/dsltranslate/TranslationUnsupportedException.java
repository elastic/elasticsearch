/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.dsltranslate;

import org.elasticsearch.index.query.QueryBuilder;

/**
 * Thrown by {@link QueryDslTranslator} when a Query DSL construct is outside the supported subset and has no faithful
 * ES|QL equivalent. The translator only reports which construct it could not translate; the <em>consumer</em> decides
 * what that costs — {@link RequestFilterRewriter} drops the clause and warns, naming the construct, rather than
 * guessing at a translation.
 *
 * <p>The reported name is Query DSL vocabulary, because it is quoted back to whoever wrote the filter. A leaf-level
 * failure knows the reason ({@code on analyzed text}) but not the clause that produced it, so it is raised with
 * {@link #forLeaf} and the DSL construct name is filled in by {@link #constructFor} at the point the clause is caught.
 */
public class TranslationUnsupportedException extends RuntimeException {

    private final String text;

    /** True when {@link #text} is a leaf reason awaiting a construct name rather than a construct name itself. */
    private final boolean leafReason;

    public TranslationUnsupportedException(String construct) {
        this(construct, false);
    }

    private TranslationUnsupportedException(String text, boolean leafReason) {
        super("Query DSL construct [" + text + "] has no ES|QL translation");
        this.text = text;
        this.leafReason = leafReason;
    }

    /**
     * A failure raised where only the reason is known — typically a type the leaf cannot be built over, or a literal
     * the type cannot represent. The DSL construct name is attached later by {@link #constructFor}.
     */
    public static TranslationUnsupportedException forLeaf(String reason) {
        return new TranslationUnsupportedException(reason, true);
    }

    /** The DSL construct name (e.g. {@code wildcard}, {@code geo_bounding_box}) that could not be translated. */
    public String construct() {
        return text;
    }

    /**
     * The construct name to report for {@code query}: the explicit one when the failure named it, otherwise the
     * query's own DSL name qualified by the reason — {@code wildcard[on analyzed text]}, never an ES|QL function name.
     */
    public String constructFor(QueryBuilder query) {
        return leafReason ? query.getName() + "[" + text + "]" : text;
    }
}
