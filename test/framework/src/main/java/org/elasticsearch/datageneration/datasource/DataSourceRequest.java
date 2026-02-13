/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

import org.elasticsearch.core.Tuple;
import org.elasticsearch.datageneration.DataGeneratorSpecification;
import org.elasticsearch.datageneration.fields.DynamicMapping;
import org.elasticsearch.index.mapper.ObjectMapper;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public interface DataSourceRequest<TResponse extends DataSourceResponse> {
    TResponse accept(DataSourceHandler handler);

    record FieldDataGenerator(String fieldName, String fieldType, DataSource dataSource)
        implements
            DataSourceRequest<DataSourceResponse.FieldDataGenerator> {
        public DataSourceResponse.FieldDataGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record LongGenerator() implements DataSourceRequest<DataSourceResponse.LongGenerator> {
        public DataSourceResponse.LongGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record UnsignedLongGenerator() implements DataSourceRequest<DataSourceResponse.UnsignedLongGenerator> {
        public DataSourceResponse.UnsignedLongGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record IntegerGenerator() implements DataSourceRequest<DataSourceResponse.IntegerGenerator> {
        public DataSourceResponse.IntegerGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ShortGenerator() implements DataSourceRequest<DataSourceResponse.ShortGenerator> {
        public DataSourceResponse.ShortGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ByteGenerator() implements DataSourceRequest<DataSourceResponse.ByteGenerator> {
        public DataSourceResponse.ByteGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record DoubleGenerator() implements DataSourceRequest<DataSourceResponse.DoubleGenerator> {
        public DataSourceResponse.DoubleGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record FloatGenerator() implements DataSourceRequest<DataSourceResponse.FloatGenerator> {
        public DataSourceResponse.FloatGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record HalfFloatGenerator() implements DataSourceRequest<DataSourceResponse.HalfFloatGenerator> {
        public DataSourceResponse.HalfFloatGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record StringGenerator() implements DataSourceRequest<DataSourceResponse.StringGenerator> {
        public DataSourceResponse.StringGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record BooleanGenerator() implements DataSourceRequest<DataSourceResponse.BooleanGenerator> {
        public DataSourceResponse.BooleanGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record InstantGenerator() implements DataSourceRequest<DataSourceResponse.InstantGenerator> {
        public DataSourceResponse.InstantGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record GeoShapeGenerator() implements DataSourceRequest<DataSourceResponse.GeoShapeGenerator> {
        public DataSourceResponse.GeoShapeGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ShapeGenerator() implements DataSourceRequest<DataSourceResponse.ShapeGenerator> {
        public DataSourceResponse.ShapeGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record GeoPointGenerator() implements DataSourceRequest<DataSourceResponse.GeoPointGenerator> {
        public DataSourceResponse.GeoPointGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record PointGenerator() implements DataSourceRequest<DataSourceResponse.PointGenerator> {
        public DataSourceResponse.PointGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record IpGenerator() implements DataSourceRequest<DataSourceResponse.IpGenerator> {
        public DataSourceResponse.IpGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record VersionStringGenerator() implements DataSourceRequest<DataSourceResponse.VersionStringGenerator> {
        public DataSourceResponse.VersionStringGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record AggregateMetricDoubleGenerator() implements DataSourceRequest<DataSourceResponse.AggregateMetricDoubleGenerator> {
        public DataSourceResponse.AggregateMetricDoubleGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record NullWrapper() implements DataSourceRequest<DataSourceResponse.NullWrapper> {
        public DataSourceResponse.NullWrapper accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ArrayWrapper() implements DataSourceRequest<DataSourceResponse.ArrayWrapper> {
        public DataSourceResponse.ArrayWrapper accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record RepeatingWrapper() implements DataSourceRequest<DataSourceResponse.RepeatingWrapper> {
        public DataSourceResponse.RepeatingWrapper accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record MalformedWrapper(Supplier<Object> malformedValues) implements DataSourceRequest<DataSourceResponse.MalformedWrapper> {
        public DataSourceResponse.MalformedWrapper accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record TransformWrapper(double transformedProportion, Function<Object, Object> transformation)
        implements
            DataSourceRequest<DataSourceResponse.TransformWrapper> {
        public DataSourceResponse.TransformWrapper accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record TransformWeightedWrapper<T>(List<Tuple<Double, Function<T, Object>>> transformations)
        implements
            DataSourceRequest<DataSourceResponse.TransformWeightedWrapper> {
        public DataSourceResponse.TransformWeightedWrapper accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ChildFieldGenerator(DataGeneratorSpecification specification)
        implements
            DataSourceRequest<DataSourceResponse.ChildFieldGenerator> {
        public DataSourceResponse.ChildFieldGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record FieldTypeGenerator() implements DataSourceRequest<DataSourceResponse.FieldTypeGenerator> {
        public DataSourceResponse.FieldTypeGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ObjectArrayGenerator() implements DataSourceRequest<DataSourceResponse.ObjectArrayGenerator> {
        public DataSourceResponse.ObjectArrayGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record LeafMappingParametersGenerator(
        DataSource dataSource,
        String fieldName,
        String fieldType,
        Set<String> eligibleCopyToFields,
        DynamicMapping dynamicMapping
    ) implements DataSourceRequest<DataSourceResponse.LeafMappingParametersGenerator> {
        public DataSourceResponse.LeafMappingParametersGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ObjectMappingParametersGenerator(boolean isRoot, boolean isNested, ObjectMapper.Subobjects parentSubobjects)
        implements
            DataSourceRequest<DataSourceResponse.ObjectMappingParametersGenerator> {
        public DataSourceResponse.ObjectMappingParametersGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record DynamicMappingGenerator() implements DataSourceRequest<DataSourceResponse.DynamicMappingGenerator> {
        public DataSourceResponse.DynamicMappingGenerator accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }
}
