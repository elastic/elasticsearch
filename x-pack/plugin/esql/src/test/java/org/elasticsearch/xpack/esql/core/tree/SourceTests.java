/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.tree;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
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

    /**
     * When a {@link Configuration} carries a {@code null} query (e.g. for pre-built plans that
     * bypass query-string parsing), the stored query is an empty string. If a plan built from a
     * real query string is later executed via such a configuration, Source objects deserialized on
     * data nodes must not throw — line/column are preserved, and the source text snippet degrades
     * gracefully to empty.
     */
    public void testReadFromWithNullQueryYieldsEmptyText() throws IOException {
        Source original = new Source(1, 1, "FROM index");
        Configuration writeConfig = randomConfiguration("FROM index | LIMIT 10");
        Configuration readConfig = randomConfiguration(null); // null → ""
        try (BytesStreamOutput out = new BytesStreamOutput(); PlanStreamOutput planOut = new PlanStreamOutput(out, writeConfig)) {
            original.writeTo(planOut);
            try (PlanStreamInput in = new PlanStreamInput(out.bytes().streamInput(), new NamedWriteableRegistry(List.of()), readConfig)) {
                Source deserialized = Source.readFrom(in);
                assertThat(deserialized.source(), equalTo(original.source()));
                assertThat(deserialized.text(), equalTo(""));
            }
        }
    }

    /**
     * Same as above but with a Source from line 2 of a multi-line query. Without the
     * {@code query.isEmpty()} early-return in {@code sourceText()}, the line-range check in
     * {@code textOffset()} would throw for an empty query — this test guards against that regression.
     */
    public void testReadFromWithNullQueryMultilineSourceYieldsEmptyText() throws IOException {
        Source original = new Source(2, 3, "LIMIT");
        Configuration writeConfig = randomConfiguration("FROM index\n| LIMIT 20");
        Configuration readConfig = randomConfiguration(null);
        try (BytesStreamOutput out = new BytesStreamOutput(); PlanStreamOutput planOut = new PlanStreamOutput(out, writeConfig)) {
            original.writeTo(planOut);
            try (PlanStreamInput in = new PlanStreamInput(out.bytes().streamInput(), new NamedWriteableRegistry(List.of()), readConfig)) {
                Source deserialized = Source.readFrom(in);
                assertThat(deserialized.source(), equalTo(original.source()));
                assertThat(deserialized.text(), equalTo(""));
            }
        }
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
