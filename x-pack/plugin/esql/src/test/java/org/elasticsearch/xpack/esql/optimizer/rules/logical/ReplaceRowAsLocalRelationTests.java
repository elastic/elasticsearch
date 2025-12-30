/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import static org.hamcrest.Matchers.stringContainsInOrder;

public class ReplaceRowAsLocalRelationTests extends AbstractLogicalPlanOptimizerTests {

    public void testMultiRowDifferentDatatypes() {
        ParsingException e = expectThrows(ParsingException.class, () -> plan("""
                ROW a=1;
                ROW a="foo"
            """));
        checkMessage(e,"a");
    }

    public void testMultiRowMultiColumDifferentDatatypes() {
        ParsingException e = expectThrows(ParsingException.class, () -> plan("""
                ROW a=1, b=2, c=3;
                ROW a=1, b="2", c=3;
            """));
        checkMessage(e,"b");
    }

    public void testMultiRowMultiColumMultivalueDifferentDatatypes() {
        ParsingException e = expectThrows(ParsingException.class, () -> plan("""
                ROW a=1, b=2, c=[3,4];
                ROW a=1, b=2, c=["3","4"];
            """));
        checkMessage(e,"c");
    }

    public void testSparseRowsDifferentDatatypes() {
        ParsingException e = expectThrows(ParsingException.class, () -> plan("""
                ROW a=1;
                ROW b=2, c=3;
                ROW a=1, b="2";
                ROW c=3
            """));
        checkMessage(e, "b");
    }

    private void checkMessage(ParsingException e, String field) {
        assertThat(e.getMessage(), stringContainsInOrder(
            "Field '"+field+"' was previously identified as of type "," but a later 'ROW","' seems to specify it as type"
        ));
    }
}
