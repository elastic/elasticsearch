/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration.datasource;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.math.BigInteger;
import java.time.Instant;

public class DefaultPrimitiveTypesHandler implements DataSourceHandler {
    @Override
    public DataSourceResponse.LongGenerator handle(DataSourceRequest.LongGenerator request) {
        return new DataSourceResponse.LongGenerator(ESTestCase::randomLong);
    }

    @Override
    public DataSourceResponse.UnsignedLongGenerator handle(DataSourceRequest.UnsignedLongGenerator request) {
        return new DataSourceResponse.UnsignedLongGenerator(() -> new BigInteger(64, ESTestCase.random()));
    }

    @Override
    public DataSourceResponse.IntegerGenerator handle(DataSourceRequest.IntegerGenerator request) {
        return new DataSourceResponse.IntegerGenerator(ESTestCase::randomInt);
    }

    @Override
    public DataSourceResponse.ShortGenerator handle(DataSourceRequest.ShortGenerator request) {
        return new DataSourceResponse.ShortGenerator(ESTestCase::randomShort);
    }

    @Override
    public DataSourceResponse.ByteGenerator handle(DataSourceRequest.ByteGenerator request) {
        return new DataSourceResponse.ByteGenerator(ESTestCase::randomByte);
    }

    @Override
    public DataSourceResponse.DoubleGenerator handle(DataSourceRequest.DoubleGenerator request) {
        return new DataSourceResponse.DoubleGenerator(ESTestCase::randomDouble);
    }

    @Override
    public DataSourceResponse.FloatGenerator handle(DataSourceRequest.FloatGenerator request) {
        return new DataSourceResponse.FloatGenerator(ESTestCase::randomFloat);
    }

    @Override
    public DataSourceResponse.HalfFloatGenerator handle(DataSourceRequest.HalfFloatGenerator request) {
        // All floats are accepted but stored precision is reduced.
        return new DataSourceResponse.HalfFloatGenerator(ESTestCase::randomFloat);
    }

    @Override
    public DataSourceResponse.StringGenerator handle(DataSourceRequest.StringGenerator request) {
        return new DataSourceResponse.StringGenerator(() -> ESTestCase.randomAlphaOfLengthBetween(0, 50));
    }

    @Override
    public DataSourceResponse.BooleanGenerator handle(DataSourceRequest.BooleanGenerator request) {
        return new DataSourceResponse.BooleanGenerator(ESTestCase::randomBoolean);
    }

    private static final Instant MAX_INSTANT = Instant.parse("2200-01-01T00:00:00Z");

    @Override
    public DataSourceResponse.InstantGenerator handle(DataSourceRequest.InstantGenerator request) {
        return new DataSourceResponse.InstantGenerator(() -> ESTestCase.randomInstantBetween(Instant.ofEpochMilli(1), MAX_INSTANT));
    }

    @Override
    public DataSourceResponse.GeoPointGenerator handle(DataSourceRequest.GeoPointGenerator request) {
        return new DataSourceResponse.GeoPointGenerator(() -> RandomGeoGenerator.randomPoint(ESTestCase.random()));
    }

    @Override
    public DataSourceResponse.IpGenerator handle(DataSourceRequest.IpGenerator request) {
        return new DataSourceResponse.IpGenerator(() -> ESTestCase.randomIp(ESTestCase.randomBoolean()));
    }
}
