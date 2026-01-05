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
import org.elasticsearch.datageneration.fields.leaf.MatchOnlyTextFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ScaledFloatFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.ShortFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.TextFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.UnsignedLongFieldDataGenerator;
import org.elasticsearch.datageneration.fields.leaf.WildcardFieldDataGenerator;

import java.util.function.BiFunction;

/**
 * Lists all leaf field types that are supported for data generation by default.
 */
public enum FieldType {
    KEYWORD("keyword", (fn, ds) -> new KeywordFieldDataGenerator(ds)),
    LONG("long", LongFieldDataGenerator::new),
    UNSIGNED_LONG("unsigned_long", UnsignedLongFieldDataGenerator::new),
    INTEGER("integer", IntegerFieldDataGenerator::new),
    SHORT("short", ShortFieldDataGenerator::new),
    BYTE("byte", ByteFieldDataGenerator::new),
    DOUBLE("double", DoubleFieldDataGenerator::new),
    FLOAT("float", FloatFieldDataGenerator::new),
    HALF_FLOAT("half_float", HalfFloatFieldDataGenerator::new),
    SCALED_FLOAT("scaled_float", ScaledFloatFieldDataGenerator::new),
    COUNTED_KEYWORD("counted_keyword", CountedKeywordFieldDataGenerator::new),
    BOOLEAN("boolean", (fn, ds) -> new BooleanFieldDataGenerator(ds)),
    DATE("date", (fn, ds) -> new DateFieldDataGenerator(ds)),
    GEO_POINT("geo_point", (fn, ds) -> new GeoPointFieldDataGenerator(ds)),
    TEXT("text", (fn, ds) -> new TextFieldDataGenerator(ds)),
    IP("ip", (fn, ds) -> new IpFieldDataGenerator(ds)),
    CONSTANT_KEYWORD("constant_keyword", (fn, ds) -> new ConstantKeywordFieldDataGenerator()),
    PASSTHROUGH("passthrough", (fn, ds) -> {
        // For now this field type does not have default generators.
        throw new IllegalArgumentException("Passthrough field type does not have a default generator");
    }),
    WILDCARD("wildcard", (fn, ds) -> new WildcardFieldDataGenerator(ds)),
    MATCH_ONLY_TEXT("match_only_text", (fn, ds) -> new MatchOnlyTextFieldDataGenerator(ds)),;

    private final String name;
    private final BiFunction<String, DataSource, FieldDataGenerator> fieldDataGenerator;

    FieldType(String name, BiFunction<String, DataSource, FieldDataGenerator> fieldDataGenerator) {
        this.name = name;
        this.fieldDataGenerator = fieldDataGenerator;
    }

    public FieldDataGenerator generator(String fieldName, DataSource dataSource) {
        return fieldDataGenerator.apply(fieldName, dataSource);
    }

    public static FieldType tryParse(String name) {
        for (FieldType fieldType : FieldType.values()) {
            if (fieldType.name.equals(name)) {
                return fieldType;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return name;
    }
}
