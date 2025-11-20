/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.type;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.type.MultiTypeEsField;
import org.elasticsearch.xpack.esql.expression.ExpressionWritables;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToBoolean;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToCartesianShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDatetime;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoPoint;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToGeoShape;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToInteger;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToIpLeadingZerosRejected;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToLong;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToVersion;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.core.type.DataType.isString;

/**
 * This test was originally based on the tests for sub-classes of EsField, like InvalidMappedFieldTests.
 * However, it needs access to the sub-classes of AbstractConvertFunction, like ToString, which are important conversion Expressions
 * used in the union-types feature.
 */
public class MultiTypeEsFieldTests extends AbstractEsFieldTypeTests<MultiTypeEsField> {

    private Configuration config;

    @Before
    public void initConfig() {
        config = randomConfiguration();
    }

    @Override
    protected Configuration config() {
        return config;
    }

    @Override
    protected MultiTypeEsField createTestInstance() {
        String name = randomAlphaOfLength(4);
        boolean toString = randomBoolean();
        DataType dataType = randomFrom(types());
        DataType toType = toString ? DataType.KEYWORD : dataType;
        Map<String, Expression> indexToConvertExpressions = randomConvertExpressions(name, toString, dataType);
        EsField.TimeSeriesFieldType tsType = randomFrom(EsField.TimeSeriesFieldType.values());
        return new MultiTypeEsField(name, toType, false, indexToConvertExpressions, tsType);
    }

    @Override
    protected MultiTypeEsField mutateInstance(MultiTypeEsField instance) throws IOException {
        String name = instance.getName();
        DataType dataType = instance.getDataType();
        Map<String, Expression> indexToConvertExpressions = instance.getIndexToConversionExpressions();
        EsField.TimeSeriesFieldType tsType = instance.getTimeSeriesFieldType();
        switch (between(0, 3)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> dataType = randomValueOtherThan(dataType, () -> randomFrom(DataType.types()));
            case 2 -> indexToConvertExpressions = mutateConvertExpressions(name, dataType, indexToConvertExpressions);
            case 3 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            default -> throw new IllegalArgumentException();
        }
        return new MultiTypeEsField(name, dataType, false, indexToConvertExpressions, tsType);
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(ExpressionWritables.allExpressions());
        entries.addAll(ExpressionWritables.unaryScalars());
        return new NamedWriteableRegistry(entries);
    }

    private static Map<String, Expression> randomConvertExpressions(String name, boolean toString, DataType dataType) {
        Map<String, Expression> indexToConvertExpressions = new HashMap<>();
        if (toString) {
            indexToConvertExpressions.put(randomAlphaOfLength(4), new ToString(Source.EMPTY, fieldAttribute(name, dataType)));
            indexToConvertExpressions.put(randomAlphaOfLength(4), new ToString(Source.EMPTY, fieldAttribute(name, DataType.KEYWORD)));
        } else {
            indexToConvertExpressions.put(randomAlphaOfLength(4), testConvertExpression(name, DataType.KEYWORD, dataType));
            indexToConvertExpressions.put(randomAlphaOfLength(4), testConvertExpression(name, dataType, dataType));
        }
        return indexToConvertExpressions;
    }

    private Map<String, Expression> mutateConvertExpressions(
        String name,
        DataType toType,
        Map<String, Expression> indexToConvertExpressions
    ) {
        return randomValueOtherThan(
            indexToConvertExpressions,
            () -> randomConvertExpressions(name, toType == DataType.KEYWORD, randomFrom(types()))
        );
    }

    private static List<DataType> types() {
        return List.of(
            DataType.BOOLEAN,
            DataType.DATETIME,
            DataType.DOUBLE,
            DataType.FLOAT,
            DataType.INTEGER,
            DataType.IP,
            DataType.KEYWORD,
            DataType.LONG,
            DataType.GEO_POINT,
            DataType.GEO_SHAPE,
            DataType.CARTESIAN_POINT,
            DataType.CARTESIAN_SHAPE,
            DataType.VERSION
        );
    }

    private static Expression testConvertExpression(String name, DataType fromType, DataType toType) {
        FieldAttribute fromField = fieldAttribute(name, fromType);
        if (isString(toType)) {
            return new ToString(Source.EMPTY, fromField);
        } else {
            return switch (toType) {
                case BOOLEAN -> new ToBoolean(Source.EMPTY, fromField);
                case DATETIME -> new ToDatetime(Source.EMPTY, fromField);
                case DOUBLE, FLOAT -> new ToDouble(Source.EMPTY, fromField);
                case INTEGER -> new ToInteger(Source.EMPTY, fromField);
                case LONG -> new ToLong(Source.EMPTY, fromField);
                case IP -> new ToIpLeadingZerosRejected(Source.EMPTY, fromField);
                case KEYWORD -> new ToString(Source.EMPTY, fromField);
                case GEO_POINT -> new ToGeoPoint(Source.EMPTY, fromField);
                case GEO_SHAPE -> new ToGeoShape(Source.EMPTY, fromField);
                case CARTESIAN_POINT -> new ToCartesianPoint(Source.EMPTY, fromField);
                case CARTESIAN_SHAPE -> new ToCartesianShape(Source.EMPTY, fromField);
                case VERSION -> new ToVersion(Source.EMPTY, fromField);
                default -> throw new UnsupportedOperationException("Conversion from " + fromType + " to " + toType + " is not supported");
            };
        }
    }

    private static FieldAttribute fieldAttribute(String name, DataType dataType) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, dataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }
}
