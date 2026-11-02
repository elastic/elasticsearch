/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.blockloader.BlockLoaderFunctionConfig;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.SerializationTestUtils;
import org.elasticsearch.xpack.esql.core.tree.AbstractNodeTestCase;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.tree.SourceTests;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.FunctionEsField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

/** Supplies valid factory-created metadata attributes to the generic node tests without exposing the private constructor. */
public class TimeSeriesMetadataAttributeTests extends AbstractNodeTestCase<TimeSeriesMetadataAttribute, Expression> {
    /** Generates either metadata representation, including its deferred read form, with consistent field and attribute types. */
    public static TimeSeriesMetadataAttribute randomTimeSeriesMetadataAttribute() {
        var attribute = TimeSeriesMetadataAttribute.create(
            SourceTests.randomSource(),
            randomBoolean() ? Set.of() : Set.of(randomAlphaOfLength(3), randomAlphaOfLength(4)),
            randomBoolean() ? null : TransportVersion.current(),
            randomFrom(Nullability.values()),
            randomBoolean()
        );
        if (randomBoolean()) {
            attribute = attribute.forSourceRead();
        }
        return (TimeSeriesMetadataAttribute) attribute.withName(randomAlphaOfLength(5)).withQualifier(randomAlphaOfLength(6));
    }

    @Override
    protected TimeSeriesMetadataAttribute randomInstance() {
        return randomTimeSeriesMetadataAttribute();
    }

    @Override
    protected TimeSeriesMetadataAttribute mutate(TimeSeriesMetadataAttribute instance) {
        return (TimeSeriesMetadataAttribute) instance.withName(instance.name() + "_other");
    }

    @Override
    protected TimeSeriesMetadataAttribute copy(TimeSeriesMetadataAttribute instance) {
        return (TimeSeriesMetadataAttribute) instance.withId(instance.id());
    }

    /** Every metadata property must survive a transformation of any other property. */
    @Override
    public void testTransform() {
        var attribute = randomInstance();
        List<Object> properties = Arrays.asList(
            attribute.parentName(),
            attribute.qualifier(),
            attribute.name(),
            attribute.field(),
            attribute.nullable(),
            attribute.id(),
            attribute.synthetic(),
            attribute.excludedFields()
        );
        assertEquals(properties, attribute.nodeProperties());
        List<Object> replacements = List.of(
            "parent",
            attribute.qualifier() + "_other",
            attribute.name() + "_other",
            new EsField("other", attribute.dataType(), Map.of(), false, EsField.TimeSeriesFieldType.DIMENSION),
            randomValueOtherThan(attribute.nullable(), () -> randomFrom(Nullability.values())),
            new NameId(),
            attribute.synthetic() == false,
            Set.of("other")
        );
        for (int i = 0; i < properties.size(); i++) {
            Object original = properties.get(i);
            Object replacement = replacements.get(i);
            var transformed = attribute.transformPropertiesOnly(Object.class, value -> value == original ? replacement : value);
            List<Object> expected = new ArrayList<>(properties);
            expected.set(i, replacement);
            assertEquals(expected, transformed.nodeProperties());
            assertEquals(attribute.source(), transformed.source());
        }
    }

    /** Metadata attributes are leaves; callers cannot replace their children. */
    @Override
    public void testReplaceChildren() {
        var exception = expectThrows(UnsupportedOperationException.class, () -> randomInstance().replaceChildrenSameSize(List.of()));
        assertEquals("this type of node doesn't have any children to replace", exception.getMessage());
    }

    public void testFactorySelectsRepresentationAtVersionBoundary() {
        var introduced = TransportVersion.fromName("promql_functional_timeseries");
        var previous = TransportVersionUtils.getPreviousVersion(introduced);
        var exclusions = Set.of("pod");
        assertEquals(DataType.KEYWORD, TimeSeriesMetadataAttribute.create(Source.EMPTY, exclusions).dataType());
        assertEquals(DataType.KEYWORD, TimeSeriesMetadataAttribute.create(Source.EMPTY, exclusions, previous).dataType());
        for (var version : List.of(introduced, TransportVersion.current())) {
            var metadata = TimeSeriesMetadataAttribute.create(Source.EMPTY, exclusions, version, Nullability.FALSE, true);
            assertEquals(DataType.TIMESERIES_DATA_TYPE, metadata.dataType());
            assertEquals(exclusions, metadata.excludedFields());
            assertEquals(Nullability.FALSE, metadata.nullable());
            assertTrue(metadata.synthetic());
        }
    }

    public void testToString() {
        TimeSeriesMetadataAttribute attr = TimeSeriesMetadataAttribute.create(Source.EMPTY, Set.of());
        assertThat(attr.toString(), equalTo("_timeseries{f}#" + attr.id()));
    }

    public void testExcludedFields() {
        Set<String> without = Set.of("foo", "bar");
        TimeSeriesMetadataAttribute attr = TimeSeriesMetadataAttribute.create(Source.EMPTY, without);
        assertEquals(without, attr.excludedFields());
    }

    public void testExcludedFieldsAreImmutable() {
        for (boolean dictionary : List.of(false, true)) {
            var excluded = new HashSet<>(Set.of("pod"));
            var attribute = dictionary
                ? TimeSeriesMetadataAttribute.create(Source.EMPTY, excluded, TransportVersion.current())
                : TimeSeriesMetadataAttribute.create(Source.EMPTY, excluded);
            int hash = attribute.hashCode();
            excluded.add("cluster");
            assertEquals(Set.of("pod"), attribute.excludedFields());
            assertEquals(hash, attribute.hashCode());
            expectThrows(UnsupportedOperationException.class, () -> attribute.excludedFields().add("cluster"));
        }
    }

    public void testDictionaryCloningAndSerialization() {
        var original = TimeSeriesMetadataAttribute.create(Source.EMPTY, Set.of("pod"), TransportVersion.current());
        var clone = (TimeSeriesMetadataAttribute) original.withQualifier("remote").withNullability(Nullability.FALSE).withId(new NameId());
        assertEquals(DataType.TIMESERIES_DATA_TYPE, clone.dataType());
        assertEquals(original.excludedFields(), clone.excludedFields());
        assertNotEquals(original.id(), clone.id());

        Attribute copy = SerializationTestUtils.serializeDeserialize(
            clone,
            (out, attribute) -> out.writeNamedWriteable(attribute),
            in -> in.readNamedWriteable(Attribute.class)
        );
        assertEquals(clone, copy);
        assertEquals(DataType.TIMESERIES_DATA_TYPE, copy.dataType());
        assertEquals(clone.excludedFields(), ((TimeSeriesMetadataAttribute) copy).excludedFields());
    }

    public void testSourceReadPreservesMetadataProperties() {
        var original = (TimeSeriesMetadataAttribute) TimeSeriesMetadataAttribute.create(Source.EMPTY, Set.of("pod"))
            .withQualifier("remote")
            .withNullability(Nullability.FALSE);
        var reader = original.forSourceRead();
        assertEquals(original.name(), reader.name());
        assertEquals(original.qualifier(), reader.qualifier());
        assertEquals(original.nullable(), reader.nullable());
        assertEquals(original.dataType(), reader.dataType());
        assertEquals(original.excludedFields(), reader.excludedFields());
        assertNotEquals(original.id(), reader.id());
        assertTrue(reader.synthetic());
        var field = (FunctionEsField) reader.field();
        assertEquals(SourceFieldMapper.NAME, field.getName());
        assertEquals(new BlockLoaderFunctionConfig.TimeSeriesMetadata(false, Set.of("pod")), field.functionConfig());

        // Deferred reads retain the type so the shard planner applies the metadata converter.
        var dictionary = TimeSeriesMetadataAttribute.create(Source.EMPTY, Set.of("pod"), TransportVersion.current());
        assertEquals(DataType.TIMESERIES_DATA_TYPE, dictionary.forSourceRead().dataType());
        assertEquals(dictionary.excludedFields(), dictionary.forSourceRead().excludedFields());
    }

    public void testEqualsSameId() {
        TimeSeriesMetadataAttribute a = TimeSeriesMetadataAttribute.create(Source.EMPTY, Set.of("x"));
        TimeSeriesMetadataAttribute b = (TimeSeriesMetadataAttribute) a.withId(a.id());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testNotEqualWithDifferentExcludedFields() {
        TimeSeriesMetadataAttribute a = TimeSeriesMetadataAttribute.create(Source.EMPTY, Set.of("x"));
        TimeSeriesMetadataAttribute b = TimeSeriesMetadataAttribute.create(Source.EMPTY, Set.of("y"));
        assertNotEquals(a, b);
    }
}
