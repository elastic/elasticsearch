/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.AbstractNamedExpressionSerializationTests;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentableInExpression;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomSerializableEsField;
import static org.hamcrest.Matchers.equalTo;

public class FieldAttributeTests extends AbstractNamedExpressionSerializationTests<FieldAttribute> {
    public static FieldAttribute createFieldAttribute(int maxDepth, boolean onlyRepresentable) {
        Source source = Source.EMPTY;
        String parentName = maxDepth == 0 || randomBoolean() ? null : randomAlphaOfLength(3);
        String qualifier = randomBoolean() ? null : randomAlphaOfLength(3);
        String name = randomAlphaOfLength(5);
        EsField field = onlyRepresentable ? randomRepresentableEsField(maxDepth) : randomSerializableEsField(maxDepth);
        Nullability nullability = randomFrom(Nullability.values());
        boolean synthetic = randomBoolean();
        return new FieldAttribute(source, parentName, qualifier, name, field, nullability, new NameId(), synthetic);
    }

    private static EsField randomRepresentableEsField(int maxDepth) {
        return randomValueOtherThanMany(
            f -> false == isRepresentableInExpression(f.getDataType()),
            () -> randomSerializableEsField(maxDepth)
        );
    }

    @Override
    protected FieldAttribute createTestInstance() {
        return createFieldAttribute(3, false);
    }

    @Override
    protected FieldAttribute mutateInstance(FieldAttribute instance) {
        Source source = instance.source();
        String parentName = instance.parentName();
        String name = instance.name();
        String qualifier = instance.qualifier();
        EsField field = instance.field();
        Nullability nullability = instance.nullable();
        NameId id = instance.id();
        boolean synthetic = instance.synthetic();
        switch (between(0, 6)) {
            case 0 -> parentName = randomValueOtherThan(parentName, () -> randomBoolean() ? null : randomAlphaOfLength(2));
            case 1 -> qualifier = randomAlphaOfLength(qualifier == null ? 3 : qualifier.length() + 1);
            case 2 -> name = randomAlphaOfLength(name.length() + 1);
            case 3 -> field = randomValueOtherThan(field, () -> randomSerializableEsField(3));
            case 4 -> nullability = randomValueOtherThan(nullability, () -> randomFrom(Nullability.values()));
            case 5 -> id = new NameId();
            case 6 -> synthetic = false == synthetic;
        }
        return new FieldAttribute(source, parentName, qualifier, name, field, nullability, id, synthetic);
    }

    @Override
    protected FieldAttribute mutateNameId(FieldAttribute instance) {
        return (FieldAttribute) instance.withId(new NameId());
    }

    @Override
    protected boolean equalityIgnoresId() {
        return false;
    }

    /**
     * The name of an attribute under a parent is sent without the parent's path when it starts with it. Check the shapes
     * that take that path and the ones that must not, both with a node that knows the shorter form and with one that doesn't.
     */
    public void testNameRelativeToParentRoundTrips() throws IOException {
        TransportVersion before = TransportVersionUtils.getPreviousVersion(FieldAttribute.ESQL_FIELD_ATTRIBUTE_NAME_WITHOUT_PARENT);
        List<FieldAttribute> attributes = List.of(
            // the usual case: the leaf is the name of the field
            keywordAttribute("a.b", "a.b.c", "c"),
            // under the parent, but the leaf is not the name of the field, like the synthetic attributes of union types
            keywordAttribute("a.b", "a.b.$$c$converted_to$keyword", "c"),
            // the leaf itself contains dots
            keywordAttribute("a.b", "a.b.c.d", "c.d"),
            // not under the parent at all
            keywordAttribute("a.b", "c", "c"),
            // shares a prefix with the parent, but not as a path
            keywordAttribute("a.b", "a.bc", "c"),
            keywordAttribute("a.b", "a.b", "b"),
            keywordAttribute("a.b", "a.b.", "b"),
            // no parent
            keywordAttribute(null, "a.b.c", "c")
        );
        for (FieldAttribute attribute : attributes) {
            for (TransportVersion version : List.of(TransportVersion.current(), before)) {
                FieldAttribute copy = copyInstance(attribute, version);
                assertThat(copy.name(), equalTo(attribute.name()));
                assertThat(copy.parentName(), equalTo(attribute.parentName()));
                assertThat(copy, equalTo(attribute));
            }
        }
    }

    /**
     * Sending the name without the parent's path saves the length of that path and its dot, less one byte for the flag.
     * The leaf name is then also already cached when the field writes its own name, which saves that string and its length.
     */
    public void testNameRelativeToParentIsSmaller() throws IOException {
        String parentName = "a.long.path.to.the.parent";
        FieldAttribute attribute = keywordAttribute(parentName, parentName + ".leaf", "leaf");
        TransportVersion before = TransportVersionUtils.getPreviousVersion(FieldAttribute.ESQL_FIELD_ATTRIBUTE_NAME_WITHOUT_PARENT);
        long saved = serializedSize(attribute, before) - serializedSize(attribute, FieldAttribute.ESQL_FIELD_ATTRIBUTE_NAME_WITHOUT_PARENT);
        long parentPathAndDot = parentName.length() + 1;
        long flag = 1;
        long fieldNameAndLength = "leaf".length() + 1;
        assertThat(saved, equalTo(parentPathAndDot - flag + fieldNameAndLength));
    }

    private static FieldAttribute keywordAttribute(String parentName, String name, String fieldName) {
        EsField field = new EsField(fieldName, DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE);
        return new FieldAttribute(Source.EMPTY, parentName, null, name, field);
    }

    private long serializedSize(FieldAttribute attribute, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            try (PlanStreamOutput planOut = new PlanStreamOutput(out, configuration())) {
                planOut.writeNamedWriteable(attribute);
                return out.bytes().length();
            }
        }
    }
}
