/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator.command.pipe;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.generator.Column;

import java.util.List;

public class ForkGeneratorTests extends ESTestCase {

    public void testSameSchemaIgnoresIndexMapped() {
        List<Column> executed = List.of(new Column("a", "keyword", null), new Column("x", "integer", null));
        List<Column> annotated = List.of(new Column("a", "keyword", null, true), new Column("x", "integer", null, false));
        assertTrue(ForkGenerator.sameSchema(executed, annotated));
    }

    public void testSameSchemaComparesNamesAndTypes() {
        List<Column> expected = List.of(new Column("a", "keyword", null), new Column("x", "integer", List.of("integer", "long")));
        assertFalse(ForkGenerator.sameSchema(List.of(new Column("a", "keyword", null)), expected));
        assertFalse(
            ForkGenerator.sameSchema(
                List.of(new Column("a", "keyword", null), new Column("y", "integer", List.of("integer", "long"))),
                expected
            )
        );
        assertFalse(
            ForkGenerator.sameSchema(
                List.of(new Column("a", "keyword", null), new Column("x", "long", List.of("integer", "long"))),
                expected
            )
        );
        assertFalse(ForkGenerator.sameSchema(List.of(new Column("a", "keyword", null), new Column("x", "integer", null)), expected));
    }
}
