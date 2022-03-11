/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SimpleVectorTileFormatterTests extends ESTestCase {

    public void testEmptyString() {
        var exception = expectThrows(IllegalArgumentException.class, () -> SimpleVectorTileFormatter.parse(""));
        assertThat(exception.getMessage(), containsString("Invalid mvt formatter parameter"));
    }

    public void testInvalid() {
        var exception = expectThrows(IllegalArgumentException.class, () -> SimpleVectorTileFormatter.parse("invalid"));
        assertThat(exception.getMessage(), containsString("Invalid mvt formatter parameter"));
    }

    public void testInvalidCombination() {
        var exception = expectThrows(IllegalArgumentException.class, () -> SimpleVectorTileFormatter.parse("1/2/3"));
        assertThat(exception.getMessage(), containsString("Zoom/X/Y combination is not valid"));
    }

    public void testLeadingWhitespace() {
        var exception = expectThrows(IllegalArgumentException.class, () -> SimpleVectorTileFormatter.parse(" 2/1/0"));
        assertThat(exception.getMessage(), containsString("Invalid mvt formatter parameter"));
    }

    public void testTrailingWhitespace() {
        var exception = expectThrows(IllegalArgumentException.class, () -> SimpleVectorTileFormatter.parse("2/1/0 "));
        assertThat(exception.getMessage(), containsString("Invalid mvt formatter parameter"));
    }

    public void testValidSimple() {
        int[] fields = SimpleVectorTileFormatter.parse("2/1/0");
        assertThat(fields.length, equalTo(5));
        assertThat(fields[0], equalTo(2));
        assertThat(fields[1], equalTo(1));
        assertThat(fields[2], equalTo(0));
        assertThat(fields[3], equalTo(SimpleVectorTileFormatter.DEFAULT_EXTENT));
        assertThat(fields[4], equalTo(SimpleVectorTileFormatter.DEFAULT_BUFFER_PIXELS));
    }

    public void testValidWithExtent() {
        int[] fields = SimpleVectorTileFormatter.parse("2/1/0@5000");
        assertThat(fields.length, equalTo(5));
        assertThat(fields[0], equalTo(2));
        assertThat(fields[1], equalTo(1));
        assertThat(fields[2], equalTo(0));
        assertThat(fields[3], equalTo(5000));
        assertThat(fields[4], equalTo(SimpleVectorTileFormatter.DEFAULT_BUFFER_PIXELS));
    }

    public void testInvalidExtent() {
        var exception = expectThrows(IllegalArgumentException.class, () -> SimpleVectorTileFormatter.parse("2/1/0@invalid"));
        assertThat(exception.getMessage(), containsString("Invalid mvt formatter parameter"));
    }

    public void testInvalidExtentValue() {
        var exception = expectThrows(IllegalArgumentException.class, () -> SimpleVectorTileFormatter.parse("2/1/0@0"));
        assertThat(exception.getMessage(), containsString("Extent is not valid"));
    }

    public void testValidWithExtentAndBuffer() {
        int[] fields = SimpleVectorTileFormatter.parse("2/1/0@5000:55");
        assertThat(fields.length, equalTo(5));
        assertThat(fields[0], equalTo(2));
        assertThat(fields[1], equalTo(1));
        assertThat(fields[2], equalTo(0));
        assertThat(fields[3], equalTo(5000));
        assertThat(fields[4], equalTo(55));
    }

    public void testValidWithBuffer() {
        int[] fields = SimpleVectorTileFormatter.parse("2/1/0:55");
        assertThat(fields.length, equalTo(5));
        assertThat(fields[0], equalTo(2));
        assertThat(fields[1], equalTo(1));
        assertThat(fields[2], equalTo(0));
        assertThat(fields[3], equalTo(SimpleVectorTileFormatter.DEFAULT_EXTENT));
        assertThat(fields[4], equalTo(55));
    }

    public void testInvalidBuffer() {
        var exception = expectThrows(IllegalArgumentException.class, () -> SimpleVectorTileFormatter.parse("2/1/0:invalid"));
        assertThat(exception.getMessage(), containsString("Invalid mvt formatter parameter"));
    }

    public void testInvalidBufferValue() {
        var exception = expectThrows(IllegalArgumentException.class, () -> SimpleVectorTileFormatter.parse("2/1/0:-1"));
        assertThat(exception.getMessage(), containsString("Invalid mvt formatter parameter"));
    }
}
