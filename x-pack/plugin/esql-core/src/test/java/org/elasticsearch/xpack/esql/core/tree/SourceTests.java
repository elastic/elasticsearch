/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.equalTo;

public class SourceTests extends ESTestCase {
    public static Source randomSource() {
        return new Source(between(1, Integer.MAX_VALUE), between(1, Integer.MAX_VALUE), randomAlphaOfLength(25));
    }

    public static Source mutate(Source source) {
        List<Function<Source, Source>> options = Arrays.asList(
            l -> new Source(
                randomValueOtherThan(l.source().getLineNumber(), () -> between(1, Integer.MAX_VALUE)),
                l.source().getColumnNumber() - 1,
                l.text()
            ),
            l -> new Source(
                l.source().getLineNumber(),
                randomValueOtherThan(l.source().getColumnNumber() - 1, () -> between(1, Integer.MAX_VALUE)),
                l.text()
            )
        );
        return randomFrom(options).apply(source);
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(
            randomSource(),
            l -> new Source(l.source().getLineNumber(), l.source().getColumnNumber() - 1, l.text()),
            SourceTests::mutate
        );
    }

    public void testReadEmpty() throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            randomSource().writeTo(out);
            int test = randomInt();
            out.writeInt(test);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Source.readEmpty(in), equalTo(Source.EMPTY));
                assertThat(in.readInt(), equalTo(test));
            }
        }
    }
}
