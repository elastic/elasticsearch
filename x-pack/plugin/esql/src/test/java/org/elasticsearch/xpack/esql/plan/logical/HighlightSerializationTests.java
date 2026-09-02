/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class HighlightSerializationTests extends AbstractLogicalPlanSerializationTests<Highlight> {

    @Override
    protected Highlight createTestInstance() {
        Source source = randomSource();
        LogicalPlan child = randomChild(0);
        String prefix = randomPrefix();
        boolean implicitQuery = randomBoolean();
        boolean derivedFields = randomBoolean();
        List<NamedExpression> fields = randomFields();
        return new Highlight(
            source,
            child,
            prefix,
            randomQuery(),
            implicitQuery,
            derivedFields,
            fields,
            randomNonNullOptions(),
            generatedFor(prefix, fields)
        );
    }

    @Override
    protected Highlight mutateInstance(Highlight instance) throws IOException {
        LogicalPlan child = instance.child();
        String prefix = instance.prefix();
        Expression query = instance.query();
        boolean implicitQuery = instance.implicitQuery();
        boolean derivedFields = instance.derivedFields();
        List<NamedExpression> fields = instance.fields();
        MapExpression options = instance.options();

        switch (between(0, 6)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> prefix = randomValueOtherThan(prefix, HighlightSerializationTests::randomPrefix);
            case 2 -> query = randomValueOtherThan(query, HighlightSerializationTests::randomQuery);
            case 3 -> implicitQuery = implicitQuery == false;
            case 4 -> derivedFields = derivedFields == false;
            case 5 -> fields = randomValueOtherThan(fields, HighlightSerializationTests::randomFields);
            case 6 -> options = randomValueOtherThan(options, HighlightSerializationTests::randomOptions);
        }
        return new Highlight(
            instance.source(),
            child,
            prefix,
            query,
            implicitQuery,
            derivedFields,
            fields,
            options,
            generatedFor(prefix, fields)
        );
    }

    public void testBackcompatOmitsFlagsWhenFalse() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Highlight.ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS);
        Highlight original = highlightWithFlags(false, false);
        Highlight copy = copyInstance(original, oldVersion);
        assertFalse(copy.implicitQuery());
        assertFalse(copy.derivedFields());
        assertThat(copy, equalTo(original));
    }

    public void testBackcompatRejectsDerivedFlags() {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Highlight.ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS);
        boolean implicitQuery = randomBoolean();
        boolean derivedFields = implicitQuery == false || randomBoolean();
        Highlight original = highlightWithFlags(implicitQuery, derivedFields);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> copyInstance(original, oldVersion));
        assertThat(
            e.getMessage(),
            containsString(
                "HIGHLIGHT with a derived query or field list is not supported in peer node's version ["
                    + oldVersion
                    + "]. Upgrade to version ["
                    + Highlight.ESQL_HIGHLIGHT_IMPLICIT_QUERY_AND_FIELDS
                    + "] or newer."
            )
        );
    }

    private static Highlight highlightWithFlags(boolean implicitQuery, boolean derivedFields) {
        Source source = randomSource();
        String prefix = randomPrefix();
        List<NamedExpression> fields = List.of();
        return new Highlight(
            source,
            new EsRelation(source, randomIdentifier(), IndexMode.STANDARD, Map.of(), Map.of(), Map.of(), List.of()),
            prefix,
            Literal.keyword(Source.EMPTY, randomIdentifier()),
            implicitQuery,
            derivedFields,
            fields,
            null,
            generatedFor(prefix, fields)
        );
    }

    private static String randomPrefix() {
        return randomFrom(Highlight.DEFAULT_PREFIX, "hl_", "h_", "");
    }

    private static List<NamedExpression> randomFields() {
        return randomList(1, 5, () -> randomReferenceAttribute(false));
    }

    private static List<Attribute> generatedFor(String prefix, List<NamedExpression> fields) {
        return Highlight.generatedAttributesFor(Source.EMPTY, prefix, fields);
    }

    // The query is nullable on the plan node (the bare form has no explicit query), so cover both cases.
    private static Expression randomQuery() {
        return randomBoolean() ? null : Literal.keyword(Source.EMPTY, randomIdentifier());
    }

    private static MapExpression randomOptions() {
        if (randomBoolean()) {
            return null;
        }
        return randomNonNullOptions();
    }

    private static MapExpression randomNonNullOptions() {
        List<Expression> entries = List.of(
            Literal.keyword(Source.EMPTY, Highlight.NUMBER_OF_FRAGMENTS),
            new Literal(Source.EMPTY, randomIntBetween(1, 10), DataType.INTEGER)
        );
        return new MapExpression(Source.EMPTY, entries);
    }
}
