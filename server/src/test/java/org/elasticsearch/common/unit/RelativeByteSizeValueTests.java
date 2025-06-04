/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RelativeByteSizeValueTests extends ESTestCase {

    public void testDeserialization() throws IOException {
        final var origin1 = new RelativeByteSizeValue(ByteSizeValue.of(between(0, 2048), randomFrom(ByteSizeUnit.values())));
        final var origin2 = new RelativeByteSizeValue(new RatioValue(randomDoubleBetween(0.0, 100.0, true)));
        final RelativeByteSizeValue target1, target2;

        try (var out = new BytesStreamOutput()) {
            origin1.writeTo(out);
            origin2.writeTo(out);
            try (var in = out.bytes().streamInput()) {
                target1 = RelativeByteSizeValue.readFrom(in);
                target2 = RelativeByteSizeValue.readFrom(in);
            }
        }

        assertTrue(origin1.isAbsolute());
        assertTrue(target1.isAbsolute());
        assertNull(origin1.getRatio());
        assertNull(target1.getRatio());
        assertEquals(origin1.getAbsolute(), target1.getAbsolute());
        assertEquals(origin1.getAbsolute().getDesiredUnit(), target1.getAbsolute().getDesiredUnit());

        assertFalse(origin2.isAbsolute());
        assertFalse(target2.isAbsolute());
        assertEquals(origin2.getRatio().getAsPercent(), target2.getRatio().getAsPercent(), 0.0);
    }

    public void testPercentage() {
        double value = randomIntBetween(0, 100);
        RelativeByteSizeValue parsed = RelativeByteSizeValue.parseRelativeByteSizeValue(value + "%", "test");
        assertThat(parsed.getRatio().getAsPercent(), equalTo(value));
        assertThat(parsed.isAbsolute(), is(false));
        assertThat(parsed.isNonZeroSize(), is(value != 0.0d));
    }

    public void testRatio() {
        double value = (double) randomIntBetween(1, 100) / 100;
        RelativeByteSizeValue parsed = RelativeByteSizeValue.parseRelativeByteSizeValue(Double.toString(value), "test");
        assertThat(parsed.getRatio().getAsRatio(), equalTo(value));
        assertThat(parsed.isAbsolute(), is(false));
        assertThat(parsed.isNonZeroSize(), is(true));
    }

    public void testAbsolute() {
        ByteSizeValue value = ByteSizeValue.of(between(0, 100), randomFrom(ByteSizeUnit.values()));
        RelativeByteSizeValue parsed = RelativeByteSizeValue.parseRelativeByteSizeValue(value.getStringRep(), "test");
        assertThat(parsed.getAbsolute(), equalTo(value));
        assertThat(parsed.isAbsolute(), is(true));
        assertThat(parsed.isNonZeroSize(), is(value.getBytes() != 0));
    }

    public void testZeroAbsolute() {
        RelativeByteSizeValue parsed = RelativeByteSizeValue.parseRelativeByteSizeValue("0", "test");
        assertThat(parsed.getAbsolute(), equalTo(ByteSizeValue.ZERO));
        assertThat(parsed.isAbsolute(), is(true));
        assertThat(parsed.isNonZeroSize(), is(false));
    }

    public void testFail() {
        assertFail("a", "unable to parse [test=a] as either percentage or bytes");
        assertFail("%", "unable to parse [test=%] as either percentage or bytes");
        assertFail("GB", "unable to parse [test=GB] as either percentage or bytes");
        assertFail("GB%", "unable to parse [test=GB%] as either percentage or bytes");
        assertFail("100 NB", "unable to parse [test=100 NB] as either percentage or bytes");
        assertFail("100 %a", "unable to parse [test=100 %a] as either percentage or bytes");
        assertFail("100 GB a", "unable to parse [test=100 GB a] as either percentage or bytes");
        assertFail("0,1 GB", "unable to parse [test=0,1 GB] as either percentage or bytes");
        assertFail("0,1", "unable to parse [test=0,1] as either percentage or bytes");
    }

    private void assertFail(String value, String failure) {
        ElasticsearchParseException exception = expectThrows(
            ElasticsearchParseException.class,
            () -> RelativeByteSizeValue.parseRelativeByteSizeValue(value, "test")
        );
        assertThat(exception.getMessage(), equalTo(failure));
    }
}
