/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.type.RandomDataTypeUtils;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;

/**
 * Utility class providing factory methods for {@link ReferenceAttribute} test instances.
 * Extracted so that test fixtures in consuming modules can build random ReferenceAttribute
 * instances without depending on the esql test artifact.
 */
public class ReferenceAttributeTestUtils {

    private ReferenceAttributeTestUtils() {}

    /**
     * Creates a random {@link ReferenceAttribute}.
     *
     * @param onlyRepresentable if true, only generates attributes with representable data types
     */
    public static ReferenceAttribute randomReferenceAttribute(boolean onlyRepresentable) {
        return randomReferenceAttribute(onlyRepresentable, null);
    }

    /**
     * Creates a random {@link ReferenceAttribute} restricted to types supported on the given transport version.
     *
     * @param onlyRepresentable if true, only generates attributes with representable data types
     * @param supportedOn if non-null, only generates attributes with data types supported on this version
     */
    public static ReferenceAttribute randomReferenceAttribute(boolean onlyRepresentable, TransportVersion supportedOn) {
        Source source = Source.EMPTY;
        String qualifier = randomBoolean() ? null : randomAlphaOfLength(3);
        String name = randomAlphaOfLength(5);
        Supplier<DataType> randomType = () -> RandomDataTypeUtils.randomSerializableDataType(supportedOn);
        DataType type = onlyRepresentable
            ? randomValueOtherThanMany(t -> false == DataType.isRepresentableInExpression(t), randomType)
            : randomType.get();
        Nullability nullability = randomFrom(Nullability.values());
        boolean synthetic = randomBoolean();
        return new ReferenceAttribute(source, qualifier, name, type, nullability, new NameId(), synthetic, null, randomLegTypes(type));
    }

    /**
     * The leg types a reference of this type must carry: a list for {@link DataType#UNION}, null for everything else.
     * The constructor refuses either one without the other, so a random generator over every type has to answer this
     * rather than pass null and hope the draw misses.
     */
    public static List<DataType> randomLegTypes(DataType type) {
        if (type != DataType.UNION) {
            return null;
        }
        List<DataType> candidates = List.of(DataType.BOOLEAN, DataType.DATETIME, DataType.DOUBLE, DataType.IP, DataType.KEYWORD);
        List<DataType> legs = new ArrayList<>(ESTestCase.randomSubsetOf(ESTestCase.between(2, candidates.size()), candidates));
        legs.sort(Comparator.comparing(DataType::typeName));
        return legs;
    }
}
