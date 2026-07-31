/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.hamcrest.Matchers.containsString;

public class MapperUtilsTests extends ESTestCase {

    /**
     * {@code ResolveEqlRelation} only produces an {@link EqlRelation} once its query has folded to a string
     * {@link Literal}, so a non-string-literal query reaching {@code mapLeaf} is a planner-invariant violation.
     * Hand-build one (an integer literal) to pin the defensive tripwire in {@code MapperUtils.eqlQueryString}.
     */
    public void testEqlRelationWithNonStringLiteralQueryThrows() {
        List<Attribute> output = List.of(new ReferenceAttribute(EMPTY, "@timestamp", KEYWORD));
        EqlRelation eql = new EqlRelation(
            EMPTY,
            new IndexPattern(EMPTY, "logs-*"),
            new Literal(EMPTY, 5, INTEGER),
            Map.of(),
            EqlRelation.Mode.EVENT,
            output
        );

        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> MapperUtils.mapLeaf(eql));
        assertThat(e.getMessage(), containsString("EQL query must be a string literal"));
    }
}
