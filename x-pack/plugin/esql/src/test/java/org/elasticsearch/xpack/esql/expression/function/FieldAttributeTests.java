/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.TimeSeriesMetadataAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.AbstractNamedExpressionSerializationTests;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.esql.core.type.DataType.isRepresentable;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomSerializableEsField;

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

    public void testMetadataProjectionAliasesUseExistingWireFormat() {
        for (var version : List.of(FieldAttribute.ESQL_TIMESERIES_METADATA_ATTRIBUTE, TransportVersion.current())) {
            var full = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of());
            var withoutPod = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("pod"));
            var withoutCluster = new TimeSeriesMetadataAttribute(Source.EMPTY, Set.of("cluster"));
            List<Expression> projections = List.of(
                new Alias(Source.EMPTY, "_timeseries", full),
                new Alias(Source.EMPTY, "_timeseries$1", withoutPod),
                new Alias(Source.EMPTY, "_timeseries$2", withoutCluster),
                withoutPod
            );
            List<Expression> copies = SerializationTestUtils.serializeDeserialize(projections, (out, values) -> {
                out.setTransportVersion(version);
                out.writeNamedWriteableCollection(values);
            }, in -> {
                in.setTransportVersion(version);
                return in.readNamedWriteableCollectionAsList(Expression.class);
            });
            assertEquals(projections, copies);
            for (int i = 0; i < 3; i++) {
                var original = (Alias) projections.get(i);
                var copy = (Alias) copies.get(i);
                assertEquals(original.id(), copy.id());
                var metadata = (TimeSeriesMetadataAttribute) copy.child();
                assertEquals(MetadataAttribute.TIMESERIES, metadata.name());
                assertEquals(((TimeSeriesMetadataAttribute) original.child()).excludedFields(), metadata.excludedFields());
            }
            assertSame(((Alias) copies.get(1)).child(), copies.get(3));
        }
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
}
