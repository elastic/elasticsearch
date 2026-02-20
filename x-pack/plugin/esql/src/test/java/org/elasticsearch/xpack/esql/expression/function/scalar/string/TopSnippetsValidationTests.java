/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class TopSnippetsValidationTests extends ESTestCase {

    public void testValidateWithLiteralQuery() {
        Expression field = fieldAttribute("body", DataType.TEXT);
        Expression query = new Literal(Source.EMPTY, new BytesRef("search terms"), DataType.KEYWORD);
        TopSnippets topSnippets = new TopSnippets(Source.synthetic("TOP_SNIPPETS(body, \"search terms\")"), field, query, null);

        Failures failures = new Failures();
        topSnippets.postOptimizationVerification(failures);

        assertThat(failures.failures(), is(empty()));
    }

    public void testValidateWithFieldQuery() {
        Expression field = fieldAttribute("body", DataType.TEXT);
        Expression query = fieldAttribute("title", DataType.KEYWORD);
        TopSnippets topSnippets = new TopSnippets(Source.synthetic("TOP_SNIPPETS(body, title)"), field, query, null);

        Failures failures = new Failures();
        topSnippets.postOptimizationVerification(failures);

        assertThat(failures.failures(), hasSize(1));
        assertThat(failures.failures().iterator().next().message(), containsString("Query must be a valid string"));
    }

    private static FieldAttribute fieldAttribute(String name, DataType type) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, type, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
