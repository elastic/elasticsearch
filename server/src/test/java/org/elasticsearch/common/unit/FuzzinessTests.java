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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;

public class FuzzinessTests extends ESTestCase {
    public void testNumerics() {
        String[] options = new String[]{"1.0", "1", "1.000000"};
        assertThat(Fuzziness.build(randomFrom(options)).asFloat(), equalTo(1f));
    }

    public void testParseFromXContent() throws IOException {
        final int iters = randomIntBetween(10, 50);
        for (int i = 0; i < iters; i++) {
            {
                float floatValue = randomFloat();
                XContentBuilder json = jsonBuilder().startObject()
                        .field(Fuzziness.X_FIELD_NAME, floatValue)
                        .endObject();
                XContentParser parser = createParser(json);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_NUMBER));
                Fuzziness fuzziness = Fuzziness.parse(parser);
                assertThat(fuzziness.asFloat(), equalTo(floatValue));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
            }
            {
                Integer intValue = frequently() ? randomIntBetween(0, 2) : randomIntBetween(0, 100);
                Float floatRep = randomFloat();
                Number value = intValue;
                if (randomBoolean()) {
                    value = new Float(floatRep += intValue);
                }
                XContentBuilder json = jsonBuilder().startObject()
                        .field(Fuzziness.X_FIELD_NAME, randomBoolean() ? value.toString() : value)
                        .endObject();
                XContentParser parser = createParser(json);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(parser.nextToken(), anyOf(equalTo(XContentParser.Token.VALUE_NUMBER), equalTo(XContentParser.Token.VALUE_STRING)));
                Fuzziness fuzziness = Fuzziness.parse(parser);
                if (value.intValue() >= 1) {
                    assertThat(fuzziness.asDistance(), equalTo(Math.min(2, value.intValue())));
                }
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
                if (intValue.equals(value)) {
                    switch (intValue) {
                        case 1:
                            assertThat(fuzziness, sameInstance(Fuzziness.ONE));
                            break;
                        case 2:
                            assertThat(fuzziness, sameInstance(Fuzziness.TWO));
                            break;
                        case 0:
                            assertThat(fuzziness, sameInstance(Fuzziness.ZERO));
                            break;
                        default:
                            break;
                    }
                }
            }
            {
                XContentBuilder json;
                boolean isDefaultAutoFuzzinessTested = randomBoolean();
                if (isDefaultAutoFuzzinessTested) {
                    json = Fuzziness.AUTO.toXContent(jsonBuilder().startObject(), null).endObject();
                } else {
                    String auto = randomBoolean() ? "AUTO" : "auto";
                    if (randomBoolean()) {
                        auto += ":" + randomIntBetween(1, 3) + "," + randomIntBetween(4, 10);
                    }
                    json = jsonBuilder().startObject()
                        .field(Fuzziness.X_FIELD_NAME, auto)
                        .endObject();
                }
                XContentParser parser = createParser(json);
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
                Fuzziness fuzziness = Fuzziness.parse(parser);
                if (isDefaultAutoFuzzinessTested) {
                    assertThat(fuzziness, sameInstance(Fuzziness.AUTO));
                }
                assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
            }
        }

    }

    public void testAuto() {
        assertThat(Fuzziness.AUTO.asFloat(), equalTo(1f));
    }

    public void testAsDistance() {
        final int iters = randomIntBetween(10, 50);
        for (int i = 0; i < iters; i++) {
            Integer integer = Integer.valueOf(randomIntBetween(0, 10));
            String value = "" + (randomBoolean() ? integer.intValue() : integer.floatValue());
            assertThat(Fuzziness.build(value).asDistance(), equalTo(Math.min(2, integer.intValue())));
        }
    }

    public void testSerialization() throws IOException {
        Fuzziness fuzziness = Fuzziness.AUTO;
        Fuzziness deserializedFuzziness = doSerializeRoundtrip(fuzziness);
        assertEquals(fuzziness, deserializedFuzziness);

        fuzziness = Fuzziness.fromEdits(randomIntBetween(0, 2));
        deserializedFuzziness = doSerializeRoundtrip(fuzziness);
        assertEquals(fuzziness, deserializedFuzziness);
    }

    public void testSerializationDefaultAuto() throws IOException {
        Fuzziness fuzziness = Fuzziness.AUTO;
        Fuzziness deserializedFuzziness = doSerializeRoundtrip(fuzziness);
        assertEquals(fuzziness, deserializedFuzziness);
        assertEquals(fuzziness.asFloat(), deserializedFuzziness.asFloat(), 0f);
    }

    public void testSerializationCustomAuto() throws IOException {
        String auto = "AUTO:4,7";
        XContentBuilder json = jsonBuilder().startObject()
            .field(Fuzziness.X_FIELD_NAME, auto)
            .endObject();

        XContentParser parser = createParser(json);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.VALUE_STRING));
        Fuzziness fuzziness = Fuzziness.parse(parser);

        Fuzziness deserializedFuzziness = doSerializeRoundtrip(fuzziness);
        assertEquals(fuzziness, deserializedFuzziness);
        assertEquals(fuzziness.asString(), deserializedFuzziness.asString());
    }

    private static Fuzziness doSerializeRoundtrip(Fuzziness in) throws IOException {
        BytesStreamOutput output = new BytesStreamOutput();
        in.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        return new Fuzziness(streamInput);
    }
}
