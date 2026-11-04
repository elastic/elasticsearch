/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.util.BytesRef;

import java.util.List;

/**
 * Thrown when a data node analyzes a BM25 query to a term the statistics pass did not count. Scoring would then
 * need statistics that do not exist, so the query fails instead.
 * <p>
 * The user cannot fix this from the query: it means nodes run different Lucene versions or analysis plugins. It
 * extends {@link IllegalStateException}, so it maps to a server error. The transport error that carries it names
 * the node.
 */
public class Bm25AnalyzerMismatchException extends IllegalStateException {

    Bm25AnalyzerMismatchException(String functionName, String field, List<BytesRef> nodeTerms, List<BytesRef> shippedTerms) {
        super(
            "["
                + functionName
                + "] cannot score ["
                + field
                + "] with BM25: the query analyzes to "
                + toStrings(nodeTerms)
                + " on this node, but the statistics were computed for "
                + toStrings(shippedTerms)
                + "; nodes may run different analysis plugins or versions"
        );
    }

    private static List<String> toStrings(List<BytesRef> terms) {
        return terms.stream().map(BytesRef::utf8ToString).toList();
    }
}
