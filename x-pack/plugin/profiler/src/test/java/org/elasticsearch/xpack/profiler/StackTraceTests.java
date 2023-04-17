/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class StackTraceTests extends ESTestCase {
    public void testDecodeFrameId() {
        String frameId = "AAAAAAAAAAUAAAAAAAAB3gAAAAAAD67u";
        // base64 encoded representation of the tuple (5, 478)
        assertEquals("AAAAAAAAAAUAAAAAAAAB3g", StackTrace.getFileIDFromStackFrameID(frameId));
        assertEquals(1027822, StackTrace.getAddressFromStackFrameID(frameId));
    }

    public void testRunlengthDecodeUniqueValues() {
        // 0 - 9 (reversed)
        String encodedFrameTypes = "AQkBCAEHAQYBBQEEAQMBAgEBAQA";
        List<Integer> actual = StackTrace.runLengthDecodeBase64Url(encodedFrameTypes, encodedFrameTypes.length(), 10);
        assertEquals(List.of(9, 8, 7, 6, 5, 4, 3, 2, 1, 0), actual);
    }

    public void testRunlengthDecodeSingleValue() {
        // "4", repeated ten times
        String encodedFrameTypes = "CgQ";
        List<Integer> actual = StackTrace.runLengthDecodeBase64Url(encodedFrameTypes, encodedFrameTypes.length(), 10);
        assertEquals(List.of(4, 4, 4, 4, 4, 4, 4, 4, 4, 4), actual);
    }

    public void testRunlengthDecodeFillsGap() {
        // "2", repeated three times
        String encodedFrameTypes = "AwI";
        List<Integer> actual = StackTrace.runLengthDecodeBase64Url(encodedFrameTypes, encodedFrameTypes.length(), 5);
        // zeroes should be appended for the last two values which are not present in the encoded representation.
        assertEquals(List.of(2, 2, 2, 0, 0), actual);
    }

    public void testRunlengthDecodeMixedValue() {
        // 4
        String encodedFrameTypes = "BQADAg";
        List<Integer> actual = StackTrace.runLengthDecodeBase64Url(encodedFrameTypes, encodedFrameTypes.length(), 8);
        assertEquals(List.of(0, 0, 0, 0, 0, 2, 2, 2), actual);
    }

    public void testCreateFromSource() {
        String ids = "AAAAAAAAAAUAAAAAAAAB3gAAAAAAD67u";
        String types = "AQI";
        // tag::noformat
        StackTrace stackTrace = StackTrace.fromSource(
            Map.of("Stacktrace",
                Map.of("frame",
                    Map.of(
                        "ids", ids,
                        "types", types)
                )
            )
        );
        // end::noformat
        assertEquals(List.of("AAAAAAAAAAUAAAAAAAAB3gAAAAAAD67u"), stackTrace.frameIds);
        assertEquals(List.of("AAAAAAAAAAUAAAAAAAAB3g"), stackTrace.fileIds);
        assertEquals(List.of(1027822), stackTrace.addressOrLines);
        assertEquals(List.of(2), stackTrace.typeIds);
    }

    public void testToXContent() throws IOException {
        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedRequest = XContentFactory.contentBuilder(contentType)
            .startObject()
            .array("address_or_lines", new int[] { 1027822 })
            .array("file_ids", "AAAAAAAAAAUAAAAAAAAB3g")
            .array("frame_ids", "AAAAAAAAAAUAAAAAAAAB3gAAAAAAD67u")
            .array("type_ids", new int[] { 2 })
            .endObject();

        XContentBuilder actualRequest = XContentFactory.contentBuilder(contentType);
        StackTrace stackTrace = new StackTrace(
            List.of(1027822),
            List.of("AAAAAAAAAAUAAAAAAAAB3g"),
            List.of("AAAAAAAAAAUAAAAAAAAB3gAAAAAAD67u"),
            List.of(2)
        );
        stackTrace.toXContent(actualRequest, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(BytesReference.bytes(expectedRequest), BytesReference.bytes(actualRequest), contentType);
    }

    public void testEquality() {
        StackTrace stackTrace = new StackTrace(
            List.of(102782),
            List.of("AAAAAAAAAAUAAAAAAAAB3g"),
            List.of("AAAAAAAAAAUAAAAAAAAB3gAAAAAAD67u"),
            List.of(2)
        );

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            stackTrace,
            (o -> new StackTrace(
                new ArrayList<>(o.addressOrLines),
                new ArrayList<>(o.fileIds),
                new ArrayList<>(o.frameIds),
                new ArrayList<>(o.typeIds)
            ))
        );
    }
}
