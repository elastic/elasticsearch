/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;
import static org.elasticsearch.xpack.esql.type.EsFieldTestUtils.randomSerializableEsField;

/**
 * Utility class providing factory methods for {@link FieldAttribute} test instances.
 * Extracted so that test fixtures in consuming modules can build random FieldAttribute
 * instances without depending on the esql test artifact.
 */
public class FieldAttributeTestUtils {

    private FieldAttributeTestUtils() {}

    /**
     * Creates a random {@link FieldAttribute} up to the given depth.
     *
     * @param maxDepth          maximum depth for nested EsField properties
     * @param onlyRepresentable if true, only generates attributes with representable data types
     */
    public static FieldAttribute createFieldAttribute(int maxDepth, boolean onlyRepresentable) {
        return createFieldAttribute(maxDepth, onlyRepresentable, null);
    }

    /**
     * Creates a random {@link FieldAttribute} whose field (including nested properties) is serializable on the
     * given transport version.
     *
     * @param maxDepth          maximum depth for nested EsField properties
     * @param onlyRepresentable if true, only generates attributes with representable data types
     * @param supportedOn       if non-null, only generates field types supported on this transport version
     */
    public static FieldAttribute createFieldAttribute(int maxDepth, boolean onlyRepresentable, TransportVersion supportedOn) {
        Source source = Source.EMPTY;
        String parentName = maxDepth == 0 || randomBoolean() ? null : randomAlphaOfLength(3);
        String qualifier = randomBoolean() ? null : randomAlphaOfLength(3);
        String name = randomAlphaOfLength(5);
        EsField field = onlyRepresentable
            ? randomRepresentableEsField(maxDepth, supportedOn)
            : randomSerializableEsField(maxDepth, supportedOn);
        Nullability nullability = randomFrom(Nullability.values());
        boolean synthetic = randomBoolean();
        return new FieldAttribute(source, parentName, qualifier, name, field, nullability, new NameId(), synthetic);
    }

    private static EsField randomRepresentableEsField(int maxDepth, TransportVersion supportedOn) {
        return randomValueOtherThanMany(
            f -> false == DataType.isRepresentable(f.getDataType()),
            () -> randomSerializableEsField(maxDepth, supportedOn)
        );
    }
}
