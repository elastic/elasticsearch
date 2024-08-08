/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.AbstractWireTestCase;
import org.elasticsearch.xpack.esql.core.tree.Node;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Superclass for serialization tests for all {@link Node} subclasses
 * @param <T>
 */
public abstract class AbstractNodeSerializationTests<T extends Node<? super T>> extends AbstractWireTestCase<T> {
    /**
     * We use a single random config for all serialization because it's pretty
     * heavy to build, especially in {@link #testConcurrentSerialization()}.
     */
    private Configuration config;

    public static Source randomSource() {
        int lineNumber = between(0, EXAMPLE_QUERY.length - 1);
        String line = EXAMPLE_QUERY[lineNumber];
        int offset = between(0, line.length() - 2);
        int length = between(1, line.length() - offset - 1);
        String text = line.substring(offset, offset + length);
        return new Source(lineNumber + 1, offset, text);
    }

    @Override
    protected final T copyInstance(T instance, TransportVersion version) throws IOException {
        return copyInstance(
            instance,
            getNamedWriteableRegistry(),
            (out, v) -> new PlanStreamOutput(out, new PlanNameRegistry(), configuration()).writeNamedWriteable(v),
            in -> {
                PlanStreamInput pin = new PlanStreamInput(in, new PlanNameRegistry(), in.namedWriteableRegistry(), configuration());
                @SuppressWarnings("unchecked")
                T deser = (T) pin.readNamedWriteable(categoryClass());
                if (alwaysEmptySource()) {
                    assertThat(deser.source(), sameInstance(Source.EMPTY));
                } else {
                    assertThat(deser.source(), equalTo(instance.source()));
                }
                return deser;
            },
            version
        );
    }

    protected abstract Class<? extends Node<?>> categoryClass();

    protected boolean alwaysEmptySource() {
        return false;
    }

    public final Configuration configuration() {
        return config;
    }

    private static final String[] EXAMPLE_QUERY = new String[] {
        "I am the very model of a modern Major-Gineral,",
        "I've information vegetable, animal, and mineral,",
        "I know the kings of England, and I quote the fights historical",
        "From Marathon to Waterloo, in order categorical;",
        "I'm very well acquainted, too, with matters mathematical,",
        "I understand equations, both the simple and quadratical,",
        "About binomial theorem I'm teeming with a lot o' news,",
        "With many cheerful facts about the square of the hypotenuse." };

    @Before
    public void initConfig() {
        config = randomConfiguration(String.join("\n", EXAMPLE_QUERY), Map.of());
    }
}
