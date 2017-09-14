/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Date;

public class ForecastTests extends AbstractSerializingTestCase<Forecast> {

    @Override
    protected Forecast parseInstance(XContentParser parser) {
        return Forecast.PARSER.apply(parser, null);
    }

    @Override
    protected Forecast createTestInstance() {
        return createTestInstance("ForecastTest");
    }

    public Forecast createTestInstance(String jobId) {
        Forecast forecast = new Forecast(jobId, randomNonNegativeLong(), new Date(randomLong()), randomNonNegativeLong());

        if (randomBoolean()) {
            forecast.setByFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            forecast.setByFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            forecast.setPartitionFieldName(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            forecast.setPartitionFieldValue(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            forecast.setModelFeature(randomAlphaOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            forecast.setForecastLower(randomDouble());
        }
        if (randomBoolean()) {
            forecast.setForecastUpper(randomDouble());
        }
        if (randomBoolean()) {
            forecast.setForecastPrediction(randomDouble());
        }

        return forecast;
    }

    @Override
    protected Reader<Forecast> instanceReader() {
        return Forecast::new;
    }

    @Override
    protected Forecast doParseInstance(XContentParser parser) throws IOException {
        return Forecast.PARSER.apply(parser, null);
    }

}
