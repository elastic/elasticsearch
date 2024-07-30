/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.logsdb.datageneration.datasource;

import org.elasticsearch.test.ESTestCase;

import java.math.BigInteger;

public class DefaultPrimitiveTypesHandler implements DataSourceHandler {
    public DataSourceResponse handle(DataSourceRequest request) {
        if (request instanceof DataSourceRequest.LongGenerator) {
            return new DataSourceResponse.LongGenerator(ESTestCase::randomLong);
        }
        if (request instanceof DataSourceRequest.UnsignedLongGenerator) {
            return new DataSourceResponse.UnsignedLongGenerator(() -> new BigInteger(64, ESTestCase.random()).toString());
        }
        if (request instanceof DataSourceRequest.IntegerGenerator) {
            return new DataSourceResponse.IntegerGenerator(ESTestCase::randomInt);
        }
        if (request instanceof DataSourceRequest.ShortGenerator) {
            return new DataSourceResponse.ShortGenerator(ESTestCase::randomShort);
        }
        if (request instanceof DataSourceRequest.ByteGenerator) {
            return new DataSourceResponse.ByteGenerator(ESTestCase::randomByte);
        }

        if (request instanceof DataSourceRequest.DoubleGenerator) {
            return new DataSourceResponse.DoubleGenerator(ESTestCase::randomDouble);
        }
        if (request instanceof DataSourceRequest.DoubleInRangeGenerator req) {
            return new DataSourceResponse.DoubleInRangeGenerator(
                () -> ESTestCase.randomDoubleBetween(req.minExclusive(), req.maxExclusive(), false)
            );
        }
        if (request instanceof DataSourceRequest.FloatGenerator) {
            return new DataSourceResponse.FloatGenerator(ESTestCase::randomFloat);
        }
        if (request instanceof DataSourceRequest.HalfFloatGenerator) {
            return new DataSourceResponse.HalfFloatGenerator(() -> ESTestCase.randomFloat());
        }

        if (request instanceof DataSourceRequest.StringGenerator) {
            return new DataSourceResponse.StringGenerator(() -> ESTestCase.randomAlphaOfLengthBetween(0, 50));
        }

        return new DataSourceResponse.NotMatched();
    }
}
