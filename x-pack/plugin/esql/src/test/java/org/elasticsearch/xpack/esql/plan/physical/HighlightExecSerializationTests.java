/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.MapExpression;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.TextEsField;
import org.elasticsearch.xpack.esql.plan.logical.Highlight;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.expression.function.ReferenceAttributeTestUtils.randomReferenceAttribute;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomTextEsField;
import static org.hamcrest.Matchers.equalTo;

public class HighlightExecSerializationTests extends AbstractPhysicalPlanSerializationTests<HighlightExec> {

    @Override
    protected HighlightExec createTestInstance() {
        Source source = randomSource();
        PhysicalPlan child = randomChild(0);
        String prefix = randomPrefix();
        List<NamedExpression> fields = randomFields();
        return new HighlightExec(
            source,
            child,
            prefix,
            randomQuery(),
            fields,
            randomNonNullOptions(),
            generatedFor(prefix, fields),
            randomIndexKey(),
            randomFieldMappings()
        );
    }

    @Override
    protected HighlightExec mutateInstance(HighlightExec instance) throws IOException {
        PhysicalPlan child = instance.child();
        String prefix = instance.prefix();
        Expression query = instance.query();
        List<NamedExpression> fields = instance.fields();
        MapExpression options = instance.options();
        Attribute indexKey = instance.indexKey();
        Map<String, TextEsField> fieldMappings = instance.fieldMappings();

        switch (between(0, 6)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> prefix = randomValueOtherThan(prefix, HighlightExecSerializationTests::randomPrefix);
            case 2 -> query = randomValueOtherThan(query, HighlightExecSerializationTests::randomQuery);
            case 3 -> fields = randomValueOtherThan(fields, HighlightExecSerializationTests::randomFields);
            case 4 -> options = randomValueOtherThan(options, HighlightExecSerializationTests::randomOptions);
            case 5 -> indexKey = randomValueOtherThan(indexKey, HighlightExecSerializationTests::randomIndexKey);
            case 6 -> fieldMappings = randomValueOtherThan(fieldMappings, HighlightExecSerializationTests::randomFieldMappings);
        }
        return new HighlightExec(
            instance.source(),
            child,
            prefix,
            query,
            fields,
            options,
            generatedFor(prefix, fields),
            indexKey,
            fieldMappings
        );
    }

    public void testBackcompatOmitsIndexKeyAndFieldMappings() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(TextEsField.TEXT_FIELD_ANALYZER);
        HighlightExec original = withIndexKeyAndFieldMappings(
            createTestInstance(),
            randomReferenceAttribute(false),
            Map.of(randomIdentifier(), randomTextEsField(0))
        );
        // The child's text fields drop their analyzers below this version too, so compare only this node.
        HighlightExec copy = copyInstance(original, oldVersion).replaceChild(original.child());
        assertThat(copy, equalTo(withIndexKeyAndFieldMappings(original, null, Map.of())));
    }

    private static HighlightExec withIndexKeyAndFieldMappings(HighlightExec h, Attribute indexKey, Map<String, TextEsField> fieldMappings) {
        return new HighlightExec(
            h.source(),
            h.child(),
            h.prefix(),
            h.query(),
            h.fields(),
            h.options(),
            h.generatedFields(),
            indexKey,
            fieldMappings
        );
    }

    // Set only when the queried indices disagree on an analyzer, so cover both cases.
    private static Attribute randomIndexKey() {
        return randomBoolean() ? null : randomReferenceAttribute(false);
    }

    // Set only for ON columns merged by FORK or UNION ALL, so cover the empty map too.
    private static Map<String, TextEsField> randomFieldMappings() {
        return randomMap(0, 3, () -> Tuple.tuple(randomIdentifier(), randomTextEsField(0)));
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

    public void testOutputIsCached() {
        PhysicalPlan plan = createTestInstance();
        assertSame(plan.output(), plan.output());
    }
}
