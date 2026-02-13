/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

public interface DataSourceHandler {
    default DataSourceResponse.FieldDataGenerator handle(DataSourceRequest.FieldDataGenerator request) {
        return null;
    }

    default DataSourceResponse.LongGenerator handle(DataSourceRequest.LongGenerator request) {
        return null;
    }

    default DataSourceResponse.UnsignedLongGenerator handle(DataSourceRequest.UnsignedLongGenerator request) {
        return null;
    }

    default DataSourceResponse.IntegerGenerator handle(DataSourceRequest.IntegerGenerator request) {
        return null;
    }

    default DataSourceResponse.ShortGenerator handle(DataSourceRequest.ShortGenerator request) {
        return null;
    }

    default DataSourceResponse.ByteGenerator handle(DataSourceRequest.ByteGenerator request) {
        return null;
    }

    default DataSourceResponse.DoubleGenerator handle(DataSourceRequest.DoubleGenerator request) {
        return null;
    }

    default DataSourceResponse.FloatGenerator handle(DataSourceRequest.FloatGenerator request) {
        return null;
    }

    default DataSourceResponse.HalfFloatGenerator handle(DataSourceRequest.HalfFloatGenerator request) {
        return null;
    }

    default DataSourceResponse.StringGenerator handle(DataSourceRequest.StringGenerator request) {
        return null;
    }

    default DataSourceResponse.BooleanGenerator handle(DataSourceRequest.BooleanGenerator request) {
        return null;
    }

    default DataSourceResponse.InstantGenerator handle(DataSourceRequest.InstantGenerator request) {
        return null;
    }

    default DataSourceResponse.GeoShapeGenerator handle(DataSourceRequest.GeoShapeGenerator request) {
        return null;
    }

    default DataSourceResponse.ShapeGenerator handle(DataSourceRequest.ShapeGenerator request) {
        return null;
    }

    default DataSourceResponse.GeoPointGenerator handle(DataSourceRequest.GeoPointGenerator request) {
        return null;
    }

    default DataSourceResponse.PointGenerator handle(DataSourceRequest.PointGenerator request) {
        return null;
    }

    default DataSourceResponse.IpGenerator handle(DataSourceRequest.IpGenerator request) {
        return null;
    }

    default DataSourceResponse.VersionStringGenerator handle(DataSourceRequest.VersionStringGenerator request) {
        return null;
    }

    default DataSourceResponse.AggregateMetricDoubleGenerator handle(DataSourceRequest.AggregateMetricDoubleGenerator request) {
        return null;
    }

    default DataSourceResponse.NullWrapper handle(DataSourceRequest.NullWrapper request) {
        return null;
    }

    default DataSourceResponse.ArrayWrapper handle(DataSourceRequest.ArrayWrapper request) {
        return null;
    }

    default DataSourceResponse.RepeatingWrapper handle(DataSourceRequest.RepeatingWrapper request) {
        return null;
    }

    default DataSourceResponse.MalformedWrapper handle(DataSourceRequest.MalformedWrapper request) {
        return null;
    }

    default DataSourceResponse.TransformWrapper handle(DataSourceRequest.TransformWrapper request) {
        return null;
    }

    default DataSourceResponse.TransformWeightedWrapper handle(DataSourceRequest.TransformWeightedWrapper<?> request) {
        return null;
    }

    default DataSourceResponse.ChildFieldGenerator handle(DataSourceRequest.ChildFieldGenerator request) {
        return null;
    }

    default DataSourceResponse.FieldTypeGenerator handle(DataSourceRequest.FieldTypeGenerator request) {
        return null;
    }

    default DataSourceResponse.ObjectArrayGenerator handle(DataSourceRequest.ObjectArrayGenerator request) {
        return null;
    }

    default DataSourceResponse.LeafMappingParametersGenerator handle(DataSourceRequest.LeafMappingParametersGenerator request) {
        return null;
    }

    default DataSourceResponse.ObjectMappingParametersGenerator handle(DataSourceRequest.ObjectMappingParametersGenerator request) {
        return null;
    }

    default DataSourceResponse.DynamicMappingGenerator handle(DataSourceRequest.DynamicMappingGenerator request) {
        return null;
    }
}
