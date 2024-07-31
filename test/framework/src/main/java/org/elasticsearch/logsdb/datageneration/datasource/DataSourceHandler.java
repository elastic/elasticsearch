/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

public interface DataSourceHandler {
    default DataSourceResponse handle(DataSourceRequest.LongGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.UnsignedLongGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.IntegerGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.ShortGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.ByteGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.DoubleGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.DoubleInRangeGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.FloatGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.HalfFloatGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.StringGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.NullWrapper request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.ArrayWrapper request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.ChildFieldGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.FieldTypeGenerator request) {
        return new DataSourceResponse.NotMatched();
    }

    default DataSourceResponse handle(DataSourceRequest.ObjectArrayGenerator request) {
        return new DataSourceResponse.NotMatched();
    }
}
