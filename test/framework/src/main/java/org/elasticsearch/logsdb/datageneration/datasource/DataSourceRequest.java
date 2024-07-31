/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;

public interface DataSourceRequest<TResponse extends DataSourceResponse> {
    DataSourceResponse accept(DataSourceHandler handler);

    record LongGenerator() implements DataSourceRequest<DataSourceResponse.LongGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record UnsignedLongGenerator() implements DataSourceRequest<DataSourceResponse.UnsignedLongGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record IntegerGenerator() implements DataSourceRequest<DataSourceResponse.IntegerGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ShortGenerator() implements DataSourceRequest<DataSourceResponse.ShortGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ByteGenerator() implements DataSourceRequest<DataSourceResponse.ByteGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record DoubleGenerator() implements DataSourceRequest<DataSourceResponse.DoubleGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record DoubleInRangeGenerator(double minExclusive, double maxExclusive)
        implements
            DataSourceRequest<DataSourceResponse.DoubleInRangeGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record FloatGenerator() implements DataSourceRequest<DataSourceResponse.FloatGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record HalfFloatGenerator() implements DataSourceRequest<DataSourceResponse.HalfFloatGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record StringGenerator() implements DataSourceRequest<DataSourceResponse.StringGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record NullWrapper() implements DataSourceRequest<DataSourceResponse.NullWrapper> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ArrayWrapper() implements DataSourceRequest<DataSourceResponse.ArrayWrapper> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ChildFieldGenerator(DataGeneratorSpecification specification)
        implements
            DataSourceRequest<DataSourceResponse.ChildFieldGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record FieldTypeGenerator() implements DataSourceRequest<DataSourceResponse.FieldTypeGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ObjectArrayGenerator() implements DataSourceRequest<DataSourceResponse.ObjectArrayGenerator> {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }
}
