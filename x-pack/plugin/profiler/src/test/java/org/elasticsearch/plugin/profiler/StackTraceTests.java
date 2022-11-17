/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.plugin.profiler;

import org.elasticsearch.test.ESTestCase;

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
        int[] actual = StackTrace.runLengthDecodeBase64UrlSimple(encodedFrameTypes, encodedFrameTypes.length(), 10);
        assertArrayEquals(new int[] { 9, 8, 7, 6, 5, 4, 3, 2, 1, 0 }, actual);
    }

    public void testRunlengthDecodeSingleValue() {
        // "4", repeated ten times
        String encodedFrameTypes = "CgQ";
        int[] actual = StackTrace.runLengthDecodeBase64UrlSimple(encodedFrameTypes, encodedFrameTypes.length(), 10);
        assertArrayEquals(new int[] { 4, 4, 4, 4, 4, 4, 4, 4, 4, 4 }, actual);
    }

    public void testRunlengthDecodeFillsGap() {
        // "2", repeated three times
        String encodedFrameTypes = "AwI";
        int[] actual = StackTrace.runLengthDecodeBase64UrlSimple(encodedFrameTypes, encodedFrameTypes.length(), 5);
        // zeroes should be appended for the last two values which are not present in the encoded representation.
        assertArrayEquals(new int[] { 2, 2, 2, 0, 0 }, actual);
    }

    public void testRunlengthDecodeMixedValue() {
        // 4
        String encodedFrameTypes = "BQADAg";
        int[] actual = StackTrace.runLengthDecodeBase64Url(encodedFrameTypes, encodedFrameTypes.length(), 8);
        assertArrayEquals(new int[] { 0, 0, 0, 0, 0, 2, 2, 2 }, actual);
    }

}
