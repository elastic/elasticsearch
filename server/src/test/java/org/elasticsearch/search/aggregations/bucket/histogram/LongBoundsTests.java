/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.time.ZoneOffset;
import java.util.function.LongSupplier;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.hamcrest.Matchers.equalTo;

public class LongBoundsTests extends ESTestCase {
    /**
     * Construct a random {@link LongBounds}.
     */
    public static LongBounds randomExtendedBounds() {
        LongBounds bounds = randomParsedExtendedBounds();
        if (randomBoolean()) {
            bounds = unparsed(bounds);
        }
        return bounds;
    }

    /**
     * Construct a random {@link LongBounds} in pre-parsed form.
     */
    public static LongBounds randomParsedExtendedBounds() {
        long maxDateValue = DateUtils.MAX_MILLIS_BEFORE_9999;
        long minDateValue = DateUtils.MAX_MILLIS_BEFORE_MINUS_9999; // beginning of year -9999
        if (randomBoolean()) {
            // Construct with one missing bound
            if (randomBoolean()) {
                return new LongBounds(null, maxDateValue);
            }
            return new LongBounds(minDateValue, null);
        }
        long a = randomLongBetween(minDateValue, maxDateValue);
        long b;
        do {
            b = randomLongBetween(minDateValue, maxDateValue);
        } while (a == b);
        long min = min(a, b);
        long max = max(a, b);
        return new LongBounds(min, max);
    }

    /**
     * Convert an extended bounds in parsed for into one in unparsed form.
     */
    public static LongBounds unparsed(LongBounds template) {
        // It'd probably be better to randomize the formatter
        DateFormatter formatter = DateFormatter.forPattern("strict_date_time").withZone(ZoneOffset.UTC);
        String minAsStr = template.getMin() == null ? null : formatter.formatMillis(template.getMin());
        String maxAsStr = template.getMax() == null ? null : formatter.formatMillis(template.getMax());
        return new LongBounds(minAsStr, maxAsStr);
    }

    public void testParseAndValidate() {
        long now = randomLong();
        LongSupplier nowInMillis = () -> now;
        DateFormatter formatter = DateFormatter.forPattern("date_optional_time");
        DocValueFormat format = new DocValueFormat.DateTime(formatter, ZoneOffset.UTC, DateFieldMapper.Resolution.MILLISECONDS);

        LongBounds expected = randomParsedExtendedBounds();
        LongBounds parsed = unparsed(expected).parseAndValidate("test", "extended_bounds", nowInMillis, format);
        // parsed won't *equal* expected because equal includes the String parts
        assertEquals(expected.getMin(), parsed.getMin());
        assertEquals(expected.getMax(), parsed.getMax());

        parsed = new LongBounds("now", null).parseAndValidate("test", "extended_bounds", nowInMillis, format);
        assertEquals(now, (long) parsed.getMin());
        assertNull(parsed.getMax());

        parsed = new LongBounds(null, "now").parseAndValidate("test", "extended_bounds", nowInMillis, format);
        assertNull(parsed.getMin());
        assertEquals(now, (long) parsed.getMax());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new LongBounds(100L, 90L).parseAndValidate("test", "extended_bounds", nowInMillis, format)
        );
        assertEquals(
            "[extended_bounds.min][100] cannot be greater than [extended_bounds.max][90] for histogram aggregation [test]",
            e.getMessage()
        );

        e = expectThrows(
            IllegalArgumentException.class,
            () -> unparsed(new LongBounds(100L, 90L)).parseAndValidate("test", "extended_bounds", nowInMillis, format)
        );
        assertEquals(
            "[extended_bounds.min][100] cannot be greater than [extended_bounds.max][90] for histogram aggregation [test]",
            e.getMessage()
        );
    }

    public void testTransportRoundTrip() throws IOException {
        LongBounds orig = randomExtendedBounds();

        BytesReference origBytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            orig.writeTo(out);
            origBytes = out.bytes();
        }

        LongBounds read;
        try (StreamInput in = origBytes.streamInput()) {
            read = new LongBounds(in);
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
        LongBounds orig = randomExtendedBounds();

        try (XContentBuilder out = JsonXContent.contentBuilder()) {
            out.startObject();
            orig.toXContent(out, ToXContent.EMPTY_PARAMS);
            out.endObject();

            try (XContentParser in = createParser(JsonXContent.jsonXContent, BytesReference.bytes(out))) {
                XContentParser.Token token = in.currentToken();
                assertNull(token);

                token = in.nextToken();
                assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

                LongBounds read = LongBounds.PARSER.apply(in, null);
                assertEquals(orig, read);
            } catch (Exception e) {
                throw new Exception("Error parsing [" + BytesReference.bytes(out).utf8ToString() + "]", e);
            }
        }
    }
}
