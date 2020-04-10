/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.tree;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class SourceTests extends ESTestCase {
    public static Source randomSource() {
        return new Source(between(1, Integer.MAX_VALUE), between(1, Integer.MAX_VALUE), randomAlphaOfLength(25));
    }

    public static Source mutate(Source source) {
        List<Function<Source, Source>> options = Arrays.asList(
            l -> new Source(
                randomValueOtherThan(l.source().getLineNumber(), () -> between(1, Integer.MAX_VALUE)),
                l.source().getColumnNumber() - 1,
                l.text()),
            l -> new Source(
                l.source().getLineNumber(),
                randomValueOtherThan(l.source().getColumnNumber() - 1, () -> between(1, Integer.MAX_VALUE)),
                l.text()));
        return randomFrom(options).apply(source);
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomSource(),
                l -> new Source(l.source().getLineNumber(), l.source().getColumnNumber() - 1, l.text()),
            SourceTests::mutate);
    }
}
