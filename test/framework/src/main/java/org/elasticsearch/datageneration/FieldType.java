/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration;

import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.fields.leaf.BooleanFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ByteFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ConstantKeywordFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.CountedKeywordFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.DateFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.DoubleFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.FloatFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.GeoPointFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.HalfFloatFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.IntegerFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.IpFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.KeywordFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.LongFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ScaledFloatFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ShortFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.TextFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.UnsignedLongFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.WildcardFieldDataGenerator;

/**
 * Lists all leaf field types that are supported for data generation by default.
 */
public enum FieldType {
    KEYWORD("keyword"),
    LONG("long"),
    UNSIGNED_LONG("unsigned_long"),
    INTEGER("integer"),
    SHORT("short"),
    BYTE("byte"),
    DOUBLE("double"),
    FLOAT("float"),
    HALF_FLOAT("half_float"),
    SCALED_FLOAT("scaled_float"),
    COUNTED_KEYWORD("counted_keyword"),
    BOOLEAN("boolean"),
    DATE("date"),
    GEO_POINT("geo_point"),
    TEXT("text"),
    IP("ip"),
    CONSTANT_KEYWORD("constant_keyword"),
    WILDCARD("wildcard");

    private final String name;

    FieldType(String name) {
        this.name = name;
    }

    public FieldDataGenerator generator(String fieldName, DataSource dataSource) {
        return switch (this) {
            case KEYWORD -> new KeywordFieldDataGenerator(dataSource);
            case LONG -> new LongFieldDataGenerator(fieldName, dataSource);
            case UNSIGNED_LONG -> new UnsignedLongFieldDataGenerator(fieldName, dataSource);
            case INTEGER -> new IntegerFieldDataGenerator(fieldName, dataSource);
            case SHORT -> new ShortFieldDataGenerator(fieldName, dataSource);
            case BYTE -> new ByteFieldDataGenerator(fieldName, dataSource);
            case DOUBLE -> new DoubleFieldDataGenerator(fieldName, dataSource);
            case FLOAT -> new FloatFieldDataGenerator(fieldName, dataSource);
            case HALF_FLOAT -> new HalfFloatFieldDataGenerator(fieldName, dataSource);
            case SCALED_FLOAT -> new ScaledFloatFieldDataGenerator(fieldName, dataSource);
            case COUNTED_KEYWORD -> new CountedKeywordFieldDataGenerator(fieldName, dataSource);
            case BOOLEAN -> new BooleanFieldDataGenerator(dataSource);
            case DATE -> new DateFieldDataGenerator(dataSource);
            case GEO_POINT -> new GeoPointFieldDataGenerator(dataSource);
            case TEXT -> new TextFieldDataGenerator(dataSource);
            case IP -> new IpFieldDataGenerator(dataSource);
            case CONSTANT_KEYWORD -> new ConstantKeywordFieldDataGenerator();
            case WILDCARD -> new WildcardFieldDataGenerator(dataSource);
        };
    }

    public static FieldType tryParse(String name) {
        return switch (name) {
            case "keyword" -> FieldType.KEYWORD;
            case "long" -> FieldType.LONG;
            case "unsigned_long" -> FieldType.UNSIGNED_LONG;
            case "integer" -> FieldType.INTEGER;
            case "short" -> FieldType.SHORT;
            case "byte" -> FieldType.BYTE;
            case "double" -> FieldType.DOUBLE;
            case "float" -> FieldType.FLOAT;
            case "half_float" -> FieldType.HALF_FLOAT;
            case "scaled_float" -> FieldType.SCALED_FLOAT;
            case "counted_keyword" -> FieldType.COUNTED_KEYWORD;
            case "boolean" -> FieldType.BOOLEAN;
            case "date" -> FieldType.DATE;
            case "geo_point" -> FieldType.GEO_POINT;
            case "text" -> FieldType.TEXT;
            case "ip" -> FieldType.IP;
            case "constant_keyword" -> FieldType.CONSTANT_KEYWORD;
            case "wildcard" -> FieldType.WILDCARD;
            default -> null;
        };
    }

    @Override
    public String toString() {
        return name;
    }
}
