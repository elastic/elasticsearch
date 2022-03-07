/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import static org.hamcrest.Matchers.equalTo;

public class SimpleVectorTileFormatterTests extends ESTestCase {
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    public void testEmptyString() {
        exception.expect(IllegalArgumentException.class);
        SimpleVectorTileFormatter.parse("");
    }

    public void testInvalid() {
        exception.expectMessage(Matchers.containsString("Invalid mvt formatter parameter"));
        SimpleVectorTileFormatter.parse("invalid");
    }

    public void testInvalidCombination() {
        exception.expectMessage(Matchers.containsString("combination is not valid"));
        SimpleVectorTileFormatter.parse("1/2/3");
    }

    public void testLeadingWhitespace() {
        exception.expectMessage(Matchers.containsString("Invalid mvt formatter parameter"));
        SimpleVectorTileFormatter.parse(" 2/1/0");
    }

    public void testTrailingWhitespace() {
        exception.expectMessage(Matchers.containsString("Invalid mvt formatter parameter"));
        SimpleVectorTileFormatter.parse("2/1/0 ");
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
        exception.expectMessage(Matchers.containsString("Invalid mvt formatter parameter"));
        SimpleVectorTileFormatter.parse("2/1/0@invalid");
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
        exception.expectMessage(Matchers.containsString("Invalid mvt formatter parameter"));
        SimpleVectorTileFormatter.parse("2/1/0:invalid");
    }
}
