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

    private final String construct;
    private final String detail;

    public TranslationUnsupportedException(String construct) {
        super("Query DSL construct [" + construct + "] has no ES|QL translation");
        this.construct = construct;
        this.detail = null;
    }

    private TranslationUnsupportedException(String detail, boolean leaf) {
        super("Query DSL construct [" + detail + "] has no ES|QL translation");
        this.construct = null;
        this.detail = detail;
    }

    /**
     * A failure raised where only the reason is known — typically a type the leaf cannot be built over. The DSL
     * construct name is attached later by {@link #constructFor}.
     */
    public static TranslationUnsupportedException forLeaf(String detail) {
        return new TranslationUnsupportedException(detail, true);
    }

    /** The DSL construct name (e.g. {@code wildcard}, {@code geo_bounding_box}) that could not be translated. */
    public String construct() {
        return construct != null ? construct : detail;
    }

    /**
     * The construct name to report for {@code query}: the explicit one when the failure named it, otherwise the
     * query's own DSL name qualified by the reason — {@code wildcard[on analyzed text]}, never an ES|QL function name.
     */
    public String constructFor(QueryBuilder query) {
        return construct != null ? construct : query.getName() + "[" + detail + "]";
    }
}
