/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

import org.elasticsearch.geometry.Geometry;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public interface DataSourceResponse {
    record FieldDataGenerator(org.elasticsearch.datageneration.FieldDataGenerator generator) implements DataSourceResponse {}

    record LongGenerator(Supplier<Long> generator) implements DataSourceResponse {}

    record UnsignedLongGenerator(Supplier<Object> generator) implements DataSourceResponse {}

    record IntegerGenerator(Supplier<Integer> generator) implements DataSourceResponse {}

    record ShortGenerator(Supplier<Short> generator) implements DataSourceResponse {}

    record ByteGenerator(Supplier<Byte> generator) implements DataSourceResponse {}

    record DoubleGenerator(Supplier<Double> generator) implements DataSourceResponse {}

    record FloatGenerator(Supplier<Float> generator) implements DataSourceResponse {}

    record HalfFloatGenerator(Supplier<Float> generator) implements DataSourceResponse {}

    record StringGenerator(Supplier<String> generator) implements DataSourceResponse {}

    record BooleanGenerator(Supplier<Boolean> generator) implements DataSourceResponse {}

    record InstantGenerator(Supplier<Instant> generator) implements DataSourceResponse {}

    record GeoShapeGenerator(Supplier<Geometry> generator) implements DataSourceResponse {}

    record ShapeGenerator(Supplier<Geometry> generator) implements DataSourceResponse {}

    record PointGenerator(Supplier<Object> generator) implements DataSourceResponse {}

    record GeoPointGenerator(Supplier<Object> generator) implements DataSourceResponse {}

    record IpGenerator(Supplier<InetAddress> generator) implements DataSourceResponse {}

    record VersionStringGenerator(Supplier<String> generator) implements DataSourceResponse {}

    record AggregateMetricDoubleGenerator(Supplier<Map<String, Number>> generator) implements DataSourceResponse {}

    record NullWrapper(Function<Supplier<Object>, Supplier<Object>> wrapper) implements DataSourceResponse {}

    record ArrayWrapper(Function<Supplier<Object>, Supplier<Object>> wrapper) implements DataSourceResponse {}

    record RepeatingWrapper(Function<Supplier<Object>, Supplier<Object>> wrapper) implements DataSourceResponse {}

    record MalformedWrapper(Function<Supplier<Object>, Supplier<Object>> wrapper) implements DataSourceResponse {}

    record TransformWrapper(Function<Supplier<Object>, Supplier<Object>> wrapper) implements DataSourceResponse {}

    record TransformWeightedWrapper(Function<Supplier<Object>, Supplier<Object>> wrapper) implements DataSourceResponse {}

    interface ChildFieldGenerator extends DataSourceResponse {
        int generateChildFieldCount();

        boolean generateDynamicSubObject();

        boolean generateNestedSubObject();

        boolean generateRegularSubObject();

        String generateFieldName();
    }

    record FieldTypeGenerator(Supplier<FieldTypeInfo> generator) implements DataSourceResponse {
        public record FieldTypeInfo(String fieldType) {}
    }

    record ObjectArrayGenerator(Supplier<Optional<Integer>> lengthGenerator) implements DataSourceResponse {}

    record LeafMappingParametersGenerator(Supplier<Map<String, Object>> mappingGenerator) implements DataSourceResponse {}

    record ObjectMappingParametersGenerator(Supplier<Map<String, Object>> mappingGenerator) implements DataSourceResponse {}

    record DynamicMappingGenerator(Function<Boolean, Boolean> generator) implements DataSourceResponse {}
}
