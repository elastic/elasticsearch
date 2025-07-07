/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.queries.SemanticQueryBuilder;
import org.elasticsearch.xpack.inference.queries.SimpleSemanticQueryRewriter;

public class SimpleSemanticQueryRewriterTests extends ESTestCase {

    public void testMatchQueryRewrite() {
        MatchQueryBuilder matchQuery = QueryBuilders.matchQuery("field", "value");
        SimpleSemanticQueryRewriter rewriter = new SimpleSemanticQueryRewriter();
        QueryBuilder expected = new SemanticQueryBuilder("field", "value");
        QueryBuilder rewritten = rewriter.rewrite(matchQuery);
        assertEquals(rewritten, expected);
    }

    public void testNoOpRewrite() {
        TermQueryBuilder termQuery = QueryBuilders.termQuery("field", "value");
        SimpleSemanticQueryRewriter rewriter = new SimpleSemanticQueryRewriter();
        QueryBuilder rewritten = rewriter.rewrite(termQuery);
        assertEquals(rewritten, termQuery);
    }
}
