/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.expression.AbstractNamedExpressionSerializationTests;

import java.io.IOException;

import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;
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
        return randomValueOtherThanMany(f -> false == isRepresentable(f.getDataType()), () -> randomSerializableEsField(maxDepth));
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
     * A {@link PotentiallyUnmappedKeywordEsField} holds only the leaf name, but older nodes match unmapped fields by the EsField name
     * (the full dotted path). Verify the leaf name round-trips on current nodes and is expanded to the full path for older ones.
     */
    public void testPotentiallyUnmappedKeywordSerializesFullPathToOldNodes() throws IOException {
        FieldAttribute attribute = new FieldAttribute(
            Source.EMPTY,
            "city",
            null,
            "city.name",
            new PotentiallyUnmappedKeywordEsField("name"),
            Nullability.TRUE,
            new NameId(),
            false
        );

        assertThat(copyInstance(attribute, TransportVersion.current()).field().getName(), equalTo("name"));

        TransportVersion old = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_unmapped_keyword_leaf_name"));
        assertThat(copyInstance(attribute, old).field().getName(), equalTo("city.name"));
    }
}
