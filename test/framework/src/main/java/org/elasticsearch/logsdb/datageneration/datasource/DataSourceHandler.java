/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

public interface DataSourceHandler {
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

    default DataSourceResponse.NullWrapper handle(DataSourceRequest.NullWrapper request) {
        return null;
    }

    default DataSourceResponse.ArrayWrapper handle(DataSourceRequest.ArrayWrapper request) {
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
}
