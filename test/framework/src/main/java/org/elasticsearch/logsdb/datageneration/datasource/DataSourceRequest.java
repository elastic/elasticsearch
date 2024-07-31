/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.logsdb.datageneration.DataGeneratorSpecification;

public interface DataSourceRequest {
    DataSourceResponse accept(DataSourceHandler handler);

    record LongGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record UnsignedLongGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record IntegerGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ShortGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ByteGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record DoubleGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record DoubleInRangeGenerator(double minExclusive, double maxExclusive) implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record FloatGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record HalfFloatGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record StringGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record NullWrapper() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ArrayWrapper() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ChildFieldGenerator(DataGeneratorSpecification specification) implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record FieldTypeGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }

    record ObjectArrayGenerator() implements DataSourceRequest {
        public DataSourceResponse accept(DataSourceHandler handler) {
            return handler.handle(this);
        }
    }
}
