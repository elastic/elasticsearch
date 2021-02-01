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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DoubleBoundsTests extends ESTestCase {
    /**
     * Construct a random {@link DoubleBounds}
     */
    public static DoubleBounds randomBounds() {
        Double min = randomDouble();
        Double max = randomValueOtherThan(min, ESTestCase::randomDouble);
        if (min > max) {
            double temp = min;
            min = max;
            max = temp;
        }
        if (randomBoolean()) {
            // Construct with one missing bound
            if (randomBoolean()) {
                return new DoubleBounds(null, max);
            }
            return new DoubleBounds(min, null);
        }
        return new DoubleBounds(min, max);
    }

    public void testTransportRoundTrip() throws IOException {
        DoubleBounds orig = randomBounds();

        BytesReference origBytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            orig.writeTo(out);
            origBytes = out.bytes();
        }

        DoubleBounds read;
        try (StreamInput in = origBytes.streamInput()) {
            read = new DoubleBounds(in);
            assertEquals("read fully", 0, in.available());
        }
        assertEquals(orig, read);

        BytesReference readBytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            read.writeTo(out);
            readBytes = out.bytes();
        }

        assertEquals(origBytes, readBytes);
    }

    public void testXContentRoundTrip() throws Exception {
        DoubleBounds orig = randomBounds();

        try (XContentBuilder out = JsonXContent.contentBuilder()) {
            out.startObject();
            orig.toXContent(out, ToXContent.EMPTY_PARAMS);
            out.endObject();

            try (XContentParser in = createParser(JsonXContent.jsonXContent, BytesReference.bytes(out))) {
                XContentParser.Token token = in.currentToken();
                assertNull(token);

                token = in.nextToken();
                assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

                DoubleBounds read = DoubleBounds.PARSER.apply(in, null);
                assertEquals(orig, read);
            } catch (Exception e) {
                throw new Exception("Error parsing [" + BytesReference.bytes(out).utf8ToString() + "]", e);
            }
        }
    }
}
