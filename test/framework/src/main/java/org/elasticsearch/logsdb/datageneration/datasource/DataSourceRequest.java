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
    record LongGenerator() implements DataSourceRequest {}

    record UnsignedLongGenerator() implements DataSourceRequest {}

    record IntegerGenerator() implements DataSourceRequest {}

    record ShortGenerator() implements DataSourceRequest {}

    record ByteGenerator() implements DataSourceRequest {}

    record DoubleGenerator() implements DataSourceRequest {}

    record DoubleInRangeGenerator(double minExclusive, double maxExclusive) implements DataSourceRequest {}

    record FloatGenerator() implements DataSourceRequest {}

    record HalfFloatGenerator() implements DataSourceRequest {}

    record StringGenerator() implements DataSourceRequest {}

    record NullWrapper() implements DataSourceRequest {}

    record ArrayWrapper() implements DataSourceRequest {}

    record ChildFieldGenerator(DataGeneratorSpecification specification) implements DataSourceRequest {}

    record ObjectArrayGenerator() implements DataSourceRequest {}
}
