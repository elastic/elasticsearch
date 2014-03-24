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
package org.elasticsearch.common.unit;

import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.number.IsCloseTo.closeTo;

public class FuzzinessTests extends ElasticsearchTestCase {

    @Test
    public void testNumerics() {
        String[] options = new String[]{"1.0", "1", "1.000000"};
        assertThat(Fuzziness.build(randomFrom(options)).asByte(), equalTo((byte) 1));
        assertThat(Fuzziness.build(randomFrom(options)).asInt(), equalTo(1));
        assertThat(Fuzziness.build(randomFrom(options)).asFloat(), equalTo(1f));
        assertThat(Fuzziness.build(randomFrom(options)).asDouble(), equalTo(1d));
        assertThat(Fuzziness.build(randomFrom(options)).asLong(), equalTo(1l));
        assertThat(Fuzziness.build(randomFrom(options)).asShort(), equalTo((short) 1));
    }

    @Test
    public void testParseFromXContent() throws IOException {
        final int iters = randomIntBetween(10, 50);
        for (int i = 0; i < iters; i++) {
            {
                XContent xcontent = XContentType.JSON.xContent();
                float floatValue = randomFloat();
                String json = jsonBuilder().startObject()
                        .field(Fuzziness.X_FIELD_NAME, floatValue)
                        .endObject().string();
                XContentParser parser = xcontent.createParser(json);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
                Fuzziness parse = Fuzziness.parse(parser);
                assertThat(parse.asFloat(), equalTo(floatValue));
                assertThat(parse.asDouble(), closeTo((double) floatValue, 0.000001));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
            }
            {
                XContent xcontent = XContentType.JSON.xContent();
                Integer intValue = frequently() ? randomIntBetween(0, 2) : randomIntBetween(0, 100);
                Float floatRep = randomFloat();
                Number value = intValue;
                if (randomBoolean()) {
                    value = new Float(floatRep += intValue);
                }
                String json = jsonBuilder().startObject()
                        .field(Fuzziness.X_FIELD_NAME, randomBoolean() ? value.toString() : value)
                        .endObject().string();
                XContentParser parser = xcontent.createParser(json);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(parser.nextToken(), anyOf(equalTo(XContentParser.Token.VALUE_NUMBER), equalTo(XContentParser.Token.VALUE_STRING)));
                Fuzziness parse = Fuzziness.parse(parser);
                assertThat(parse.asInt(), equalTo(value.intValue()));
                assertThat((int) parse.asShort(), equalTo(value.intValue()));
                assertThat((int) parse.asByte(), equalTo(value.intValue()));
                assertThat(parse.asLong(), equalTo(value.longValue()));
                if (value.intValue() >= 1) {
                    assertThat(parse.asDistance(), equalTo(Math.min(2, value.intValue())));
                }
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                if (intValue.equals(value)) {
                    switch (intValue) {
                        case 1:
                            assertThat(parse, sameInstance(Fuzziness.ONE));
                            break;
                        case 2:
                            assertThat(parse, sameInstance(Fuzziness.TWO));
                            break;
                        case 0:
                            assertThat(parse, sameInstance(Fuzziness.ZERO));
                            break;
                        default:
                            break;
                    }
                }
            }
            {
                XContent xcontent = XContentType.JSON.xContent();
                String json = jsonBuilder().startObject()
                        .field(Fuzziness.X_FIELD_NAME, randomBoolean() ? "AUTO" : "auto")
                        .endObject().string();
                if (randomBoolean()) {
                    json = Fuzziness.AUTO.toXContent(jsonBuilder().startObject(), null).endObject().string();
                }
                XContentParser parser = xcontent.createParser(json);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
                Fuzziness parse = Fuzziness.parse(parser);
                assertThat(parse, sameInstance(Fuzziness.AUTO));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
            }

            {
                String[] values = new String[]{"d", "H", "ms", "s", "S", "w"};
                String actual = randomIntBetween(1, 3) + randomFrom(values);
                XContent xcontent = XContentType.JSON.xContent();
                String json = jsonBuilder().startObject()
                        .field(Fuzziness.X_FIELD_NAME, actual)
                        .endObject().string();
                XContentParser parser = xcontent.createParser(json);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
                Fuzziness parse = Fuzziness.parse(parser);
                assertThat(parse.asTimeValue(), equalTo(TimeValue.parseTimeValue(actual, null)));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
            }
        }

    }

    @Test
    public void testAuto() {
        final int codePoints = randomIntBetween(0, 10);
        String string = randomRealisticUnicodeOfCodepointLength(codePoints);
        if (codePoints <= 2) {
            assertThat(Fuzziness.AUTO.asDistance(string), equalTo(0));
            assertThat(Fuzziness.fromSimilarity(Fuzziness.AUTO.asSimilarity(string)).asDistance(string), equalTo(0));
        } else if (codePoints > 5) {
            assertThat(Fuzziness.AUTO.asDistance(string), equalTo(2));
            assertThat(Fuzziness.fromSimilarity(Fuzziness.AUTO.asSimilarity(string)).asDistance(string), equalTo(2));
        } else {
            assertThat(Fuzziness.AUTO.asDistance(string), equalTo(1));
            assertThat(Fuzziness.fromSimilarity(Fuzziness.AUTO.asSimilarity(string)).asDistance(string), equalTo(1));
        }
        assertThat(Fuzziness.AUTO.asByte(), equalTo((byte) 1));
        assertThat(Fuzziness.AUTO.asInt(), equalTo(1));
        assertThat(Fuzziness.AUTO.asFloat(), equalTo(1f));
        assertThat(Fuzziness.AUTO.asDouble(), equalTo(1d));
        assertThat(Fuzziness.AUTO.asLong(), equalTo(1l));
        assertThat(Fuzziness.AUTO.asShort(), equalTo((short) 1));
        assertThat(Fuzziness.AUTO.asTimeValue(), equalTo(TimeValue.parseTimeValue("1", TimeValue.timeValueMillis(1))));

    }

    @Test
    public void testAsDistance() {
        final int iters = randomIntBetween(10, 50);
        for (int i = 0; i < iters; i++) {
            Integer integer = Integer.valueOf(randomIntBetween(0, 10));
            String value = "" + (randomBoolean() ? integer.intValue() : integer.floatValue());
            assertThat(Fuzziness.build(value).asDistance(), equalTo(Math.min(2, integer.intValue())));
        }
    }

    @Test
    public void testSimilarityToDistance() {
        assertThat(Fuzziness.fromSimilarity(0.5f).asDistance("ab"), equalTo(1));
        assertThat(Fuzziness.fromSimilarity(0.66f).asDistance("abcefg"), equalTo(2));
        assertThat(Fuzziness.fromSimilarity(0.8f).asDistance("ab"), equalTo(0));
        assertThat(Fuzziness.fromSimilarity(0.8f).asDistance("abcefg"), equalTo(1));
        assertThat((double) Fuzziness.ONE.asSimilarity("abcefg"), closeTo(0.8f, 0.05));
        assertThat((double) Fuzziness.TWO.asSimilarity("abcefg"), closeTo(0.66f, 0.05));
        assertThat((double) Fuzziness.ONE.asSimilarity("ab"), closeTo(0.5f, 0.05));

        int iters = randomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            Fuzziness fuzziness = Fuzziness.fromEdits(between(1, 2));
            String string = rarely() ? randomRealisticUnicodeOfLengthBetween(2, 4) :
                    randomRealisticUnicodeOfLengthBetween(4, 10);
            float similarity = fuzziness.asSimilarity(string);
            if (similarity != 0.0f) {
                Fuzziness similarityBased = Fuzziness.build(similarity);
                assertThat((double) similarityBased.asSimilarity(string), closeTo(similarity, 0.05));
                assertThat(similarityBased.asDistance(string), equalTo(Math.min(2, fuzziness.asDistance(string))));
            }
        }
    }
}
