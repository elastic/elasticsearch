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
import org.elasticsearch.xpack.esql.core.type.CompactMultiTypeEsField;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
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

public class CompactMultiTypeEsFieldTests extends AbstractEsFieldTypeTests<CompactMultiTypeEsField> {
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
    protected CompactMultiTypeEsField createTestInstance() {
        String name = randomAlphaOfLength(4);
        boolean toString = randomBoolean();
        DataType dataType = randomFrom(types());
        DataType toType = toString ? DataType.KEYWORD : dataType;
        Map<DataType, Expression> typeToConvertExpressions = randomConvertExpressions(name, toString, dataType);
        Expression unmappedConversionExpression = randomBoolean() ? null : createToString(name, dataType);

        EsField.TimeSeriesFieldType tsType = randomFrom(EsField.TimeSeriesFieldType.values());

        return new CompactMultiTypeEsField(name, toType, false, typeToConvertExpressions, tsType, unmappedConversionExpression);
    }

    @Override
    protected CompactMultiTypeEsField mutateInstance(CompactMultiTypeEsField instance) throws IOException {
        String name = instance.getName();
        DataType dataType = instance.getDataType();
        Map<DataType, Expression> typeToConvertExpressions = instance.getTypeToConversionExpressions();
        EsField.TimeSeriesFieldType tsType = instance.getTimeSeriesFieldType();
        Expression unmappedConversionExpression = instance.getUnmappedConversionExpression();
        switch (between(0, 4)) {
            case 0 -> name = randomAlphaOfLength(name.length() + 1);
            case 1 -> dataType = randomValueOtherThan(dataType, () -> randomFrom(DataType.types()));
            case 2 -> typeToConvertExpressions = mutateConvertExpressions(name, dataType, typeToConvertExpressions);
            case 3 -> tsType = randomValueOtherThan(tsType, () -> randomFrom(EsField.TimeSeriesFieldType.values()));
            case 4 -> unmappedConversionExpression = unmappedConversionExpression != null ? null : createToString(name, dataType);
            default -> throw new IllegalArgumentException();
        }
        return new CompactMultiTypeEsField(name, dataType, false, typeToConvertExpressions, tsType, unmappedConversionExpression);
    }

    @Override
    protected final NamedWriteableRegistry getNamedWriteableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(ExpressionWritables.allExpressions());
        entries.addAll(ExpressionWritables.unaryScalars());
        return new NamedWriteableRegistry(entries);
    }

    private Map<DataType, Expression> randomConvertExpressions(String name, boolean toString, DataType dataType) {
        Map<DataType, Expression> typeToConvertExpressions = new HashMap<>();
        if (toString) {
            typeToConvertExpressions.put(DataType.KEYWORD, createToString(name, DataType.KEYWORD));
            typeToConvertExpressions.put(dataType, createToString(name, dataType));
        } else {
            typeToConvertExpressions.put(DataType.KEYWORD, testConvertExpression(name, DataType.KEYWORD, dataType));
            typeToConvertExpressions.put(dataType, testConvertExpression(name, dataType, dataType));
        }
        return typeToConvertExpressions;
    }

    private Map<DataType, Expression> mutateConvertExpressions(
        String name,
        DataType toType,
        Map<DataType, Expression> typeToConvertExpressions
    ) {
        return randomValueOtherThan(
            typeToConvertExpressions,
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

    private Expression testConvertExpression(String name, DataType fromType, DataType toType) {
        FieldAttribute fromField = fieldAttribute(name, fromType);
        return switch (toType) {
            case BOOLEAN -> new ToBoolean(Source.EMPTY, fromField);
            case DATETIME -> new ToDatetime(Source.EMPTY, fromField, config());
            case DOUBLE, FLOAT -> new ToDouble(Source.EMPTY, fromField);
            case INTEGER -> new ToInteger(Source.EMPTY, fromField);
            case LONG -> new ToLong(Source.EMPTY, fromField);
            case IP -> new ToIpLeadingZerosRejected(Source.EMPTY, fromField);
            case KEYWORD, TEXT -> new ToString(Source.EMPTY, fromField, config());
            case GEO_POINT -> new ToGeoPoint(Source.EMPTY, fromField);
            case GEO_SHAPE -> new ToGeoShape(Source.EMPTY, fromField);
            case CARTESIAN_POINT -> new ToCartesianPoint(Source.EMPTY, fromField);
            case CARTESIAN_SHAPE -> new ToCartesianShape(Source.EMPTY, fromField);
            case VERSION -> new ToVersion(Source.EMPTY, fromField);
            default -> throw new UnsupportedOperationException("Conversion from " + fromType + " to " + toType + " is not supported");
        };
    }

    private static FieldAttribute fieldAttribute(String name, DataType dataType) {
        return new FieldAttribute(Source.EMPTY, name, new EsField(name, dataType, Map.of(), true, EsField.TimeSeriesFieldType.NONE));
    }

    private ToString createToString(String name, DataType dataType) {
        return new ToString(Source.EMPTY, fieldAttribute(name, dataType), config());
    }
}
