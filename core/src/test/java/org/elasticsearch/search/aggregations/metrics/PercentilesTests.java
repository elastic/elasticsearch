/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.metrics.percentiles.PercentilesAggregationBuilder;

import java.io.IOException;

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
        }
        if (randomBoolean()) {
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

    public void testExceptionMultipleMethods() throws IOException {
        final String illegalAgg = "{\n" +
            "       \"percentiles\": {\n" +
            "           \"field\": \"load_time\",\n" +
            "           \"percents\": [99],\n" +
            "           \"tdigest\": {\n" +
            "               \"compression\": 200\n" +
            "           },\n" +
            "           \"hdr\": {\n" +
            "               \"number_of_significant_value_digits\": 3\n" +
            "           }\n" +
            "   }\n" +
            "}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, illegalAgg);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        ParsingException e = expectThrows(ParsingException.class,
                () -> PercentilesAggregationBuilder.parse("myPercentiles", parser));
        assertEquals(
                "ParsingException[[percentiles] failed to parse field [hdr]]; "
                + "nested: IllegalStateException[Only one percentiles method should be declared.];; "
                + "java.lang.IllegalStateException: Only one percentiles method should be declared.",
                e.getDetailedMessage());
    }
}
