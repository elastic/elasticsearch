/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class PercentilesTests extends BaseAggregationTestCase<PercentilesAggregationBuilder> {

    @Override
    protected PercentilesAggregationBuilder createTestAggregatorBuilder() {
        PercentilesAggregationBuilder factory = new PercentilesAggregationBuilder(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }
        if (randomBoolean()) {
            int percentsSize = randomIntBetween(1, 20);
            double[] percents = new double[percentsSize];
            for (int i = 0; i < percentsSize; i++) {
                percents[i] = randomDouble() * 100;
            }
            factory.percentiles(percents);
        }
        if (randomBoolean()) {
            factory.numberOfSignificantValueDigits(randomIntBetween(0, 5));
        } else if (randomBoolean()) {
            factory.compression(randomIntBetween(1, 50000));
        }
        String field = randomNumericField();
        randomFieldOrScript(factory, field);
        if (randomBoolean()) {
            factory.missing("MISSING");
        }
        if (randomBoolean()) {
            factory.format("###.00");
        }
        return factory;
    }

    public void testNullOrEmptyPercentilesThrows() throws IOException {
        PercentilesAggregationBuilder builder = new PercentilesAggregationBuilder("testAgg");
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> builder.percentiles(null));
        assertEquals("[percents] must not be null: [testAgg]", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> builder.percentiles(new double[0]));
        assertEquals("[percents] must not be empty: [testAgg]", ex.getMessage());
    }

    public void testOutOfRangePercentilesThrows() throws IOException {
        PercentilesAggregationBuilder builder = new PercentilesAggregationBuilder("testAgg");
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> builder.percentiles(-0.4));
        assertEquals("percent must be in [0,100], got [-0.4]: [testAgg]", ex.getMessage());

        ex = expectThrows(IllegalArgumentException.class, () -> builder.percentiles(104));
        assertEquals("percent must be in [0,100], got [104.0]: [testAgg]", ex.getMessage());
    }

    public void testDuplicatePercentilesThrows() throws IOException {
        PercentilesAggregationBuilder builder = new PercentilesAggregationBuilder("testAgg");
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> builder.percentiles(5, 42, 10, 99, 42, 87));
        assertEquals("percent [42.0] has been specified twice: [testAgg]", ex.getMessage());
    }

    public void testExceptionMultipleMethods() throws IOException {
        final String illegalAgg = """
            {
                   "percentiles": {
                       "field": "load_time",
                       "percents": [99],
                       "tdigest": {
                           "compression": 200
                       },
                       "hdr": {
                           "number_of_significant_value_digits": 3
                       }
               }
            }""";
        XContentParser parser = createParser(JsonXContent.jsonXContent, illegalAgg);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        XContentParseException e = expectThrows(
            XContentParseException.class,
            () -> PercentilesAggregationBuilder.PARSER.parse(parser, "myPercentiles")
        );
        assertThat(e.getMessage(), containsString("[percentiles] failed to parse field [hdr]"));
    }

    public void testParseTDigestWithParams() throws IOException {
        final String percentileConfig = """
            {
               "percentiles": {
                   "field": "load_time",
                   "percents": [1, 99],
                   "tdigest": {
                       "compression": 200,
                       "execution_hint": "high_accuracy"
                   }
               }
            }""";
        XContentParser parser = createParser(JsonXContent.jsonXContent, percentileConfig);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());

        PercentilesAggregationBuilder parsed = PercentilesAggregationBuilder.PARSER.parse(parser, "myPercentiles");
        assertArrayEquals(parsed.percentiles(), new double[] { 1.0, 99.0 }, 0.0);
        assertEquals(PercentilesMethod.TDIGEST, parsed.percentilesConfig().getMethod());
        var tdigestConfig = (PercentilesConfig.TDigest) parsed.percentilesConfig();
        assertEquals(200.0, tdigestConfig.getCompression(), 0);
        assertEquals(TDigestState.ExecutionHint.HIGH_ACCURACY, TDigestState.ExecutionHint.parse(tdigestConfig.getExecutionHint()));
    }
}
