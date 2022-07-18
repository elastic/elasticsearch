/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.List;

import static org.elasticsearch.xpack.ql.tree.Source.EMPTY;

public class StatementParserTests extends ESTestCase {

    EsqlParser parser = new EsqlParser();

    public void testRowCommand() {
        assertEquals(
            new Row(
                EMPTY,
                List.of(
                    new Alias(EMPTY, "a", new Literal(EMPTY, 1, DataTypes.INTEGER)),
                    new Alias(EMPTY, "b", new Literal(EMPTY, 2, DataTypes.INTEGER))
                )
            ),
            parser.createStatement("row a = 1, b = 2")
        );
    }

    public void testRowCommandImplicitFieldName() {
        assertEquals(
            new Row(
                EMPTY,
                List.of(
                    new Alias(EMPTY, "1", new Literal(EMPTY, 1, DataTypes.INTEGER)),
                    new Alias(EMPTY, "2", new Literal(EMPTY, 2, DataTypes.INTEGER)),
                    new Alias(EMPTY, "c", new Literal(EMPTY, 3, DataTypes.INTEGER))
                )
            ),
            parser.createStatement("row 1, 2, c = 3")
        );
    }

}
