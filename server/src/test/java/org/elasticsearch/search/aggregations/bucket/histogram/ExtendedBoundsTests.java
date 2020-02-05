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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.ZoneOffset;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.hamcrest.Matchers.equalTo;

public class ExtendedBoundsTests extends ESTestCase {
    /**
     * Construct a random {@link ExtendedBounds}.
     */
    public static ExtendedBounds randomExtendedBounds() {
        ExtendedBounds bounds = randomParsedExtendedBounds();
        if (randomBoolean()) {
            bounds = unparsed(bounds);
        }
        return bounds;
    }

    /**
     * Construct a random {@link ExtendedBounds} in pre-parsed form.
     */
    public static ExtendedBounds randomParsedExtendedBounds() {
        long maxDateValue = 253402300799999L; // end of year 9999
        long minDateValue = -377705116800000L; // beginning of year -9999
        if (randomBoolean()) {
            // Construct with one missing bound
            if (randomBoolean()) {
                return new ExtendedBounds(null, maxDateValue);
            }
            return new ExtendedBounds(minDateValue, null);
        }
        long a = randomLongBetween(minDateValue, maxDateValue);
        long b;
        do {
            b = randomLongBetween(minDateValue, maxDateValue);
        } while (a == b);
        long min = min(a, b);
        long max = max(a, b);
        return new ExtendedBounds(min, max);
    }

    /**
     * Convert an extended bounds in parsed for into one in unparsed form.
     */
    public static ExtendedBounds unparsed(ExtendedBounds template) {
        // It'd probably be better to randomize the formatter
        DateFormatter formatter = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);
        String minAsStr = template.getMin() == null ? null : formatter.formatMillis(template.getMin());
        String maxAsStr = template.getMax() == null ? null : formatter.formatMillis(template.getMax());
        return new ExtendedBounds(minAsStr, maxAsStr);
    }

    public void testParseAndValidate() {
        long now = randomLong();
        Settings indexSettings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).build();
        QueryShardContext qsc = new QueryShardContext(0,
                new IndexSettings(IndexMetaData.builder("foo").settings(indexSettings).build(), indexSettings),
                BigArrays.NON_RECYCLING_INSTANCE, null, null, null, null, null, xContentRegistry(), writableRegistry(),
                null, null, () -> now, null, null);
        DateFormatter formatter = DateFormatter.forPattern("dateOptionalTime");
        DocValueFormat format = new DocValueFormat.DateTime(formatter, ZoneOffset.UTC, DateFieldMapper.Resolution.MILLISECONDS);

        ExtendedBounds expected = randomParsedExtendedBounds();
        ExtendedBounds parsed = unparsed(expected).parseAndValidate("test", qsc, format);
        // parsed won't *equal* expected because equal includes the String parts
        assertEquals(expected.getMin(), parsed.getMin());
        assertEquals(expected.getMax(), parsed.getMax());

        parsed = new ExtendedBounds("now", null).parseAndValidate("test", qsc, format);
        assertEquals(now, (long) parsed.getMin());
        assertNull(parsed.getMax());

        parsed = new ExtendedBounds(null, "now").parseAndValidate("test", qsc, format);
        assertNull(parsed.getMin());
        assertEquals(now, (long) parsed.getMax());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new ExtendedBounds(100L, 90L).parseAndValidate("test", qsc, format));
        assertEquals("[extended_bounds.min][100] cannot be greater than [extended_bounds.max][90] for histogram aggregation [test]",
                e.getMessage());

        e = expectThrows(IllegalArgumentException.class,
                () -> unparsed(new ExtendedBounds(100L, 90L)).parseAndValidate("test", qsc, format));
        assertEquals("[extended_bounds.min][100] cannot be greater than [extended_bounds.max][90] for histogram aggregation [test]",
                e.getMessage());
    }

    public void testTransportRoundTrip() throws IOException {
        ExtendedBounds orig = randomExtendedBounds();

        BytesReference origBytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            orig.writeTo(out);
            origBytes = out.bytes();
        }

        ExtendedBounds read;
        try (StreamInput in = origBytes.streamInput()) {
            read = new ExtendedBounds(in);
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
        ExtendedBounds orig = randomExtendedBounds();

        try (XContentBuilder out = JsonXContent.contentBuilder()) {
            out.startObject();
            orig.toXContent(out, ToXContent.EMPTY_PARAMS);
            out.endObject();

            try (XContentParser in = createParser(JsonXContent.jsonXContent, BytesReference.bytes(out))) {
                XContentParser.Token token = in.currentToken();
                assertNull(token);

                token = in.nextToken();
                assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

                token = in.nextToken();
                assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
                assertThat(in.currentName(), equalTo(ExtendedBounds.EXTENDED_BOUNDS_FIELD.getPreferredName()));

                ExtendedBounds read = ExtendedBounds.PARSER.apply(in, null);
                assertEquals(orig, read);
            } catch (Exception e) {
                throw new Exception("Error parsing [" + BytesReference.bytes(out).utf8ToString() + "]", e);
            }
        }
    }
}
