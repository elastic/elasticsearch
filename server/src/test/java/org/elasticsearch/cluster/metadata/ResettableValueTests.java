/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class ResettableValueTests extends ESTestCase {

    public void testCreateConstructor() {
        ResettableValue<Boolean> value = ResettableValue.create(true);
        assertThat(value.get(), equalTo(true));
        assertThat(value.isDefined(), equalTo(true));

        assertThat(ResettableValue.create(null), equalTo(ResettableValue.undefined()));
    }

    public void testMap() {
        Function<Integer, Integer> increment = x -> x + 1;
        assertThat(ResettableValue.<Integer>undefined().map(increment), equalTo(ResettableValue.undefined()));
        assertThat(ResettableValue.<Integer>reset().map(increment), equalTo(ResettableValue.reset()));
        int value = randomIntBetween(0, Integer.MAX_VALUE - 1);
        assertThat(ResettableValue.create(value).map(increment), equalTo(ResettableValue.create(value + 1)));
    }

    public void testMapAndGet() {
        Function<Integer, Integer> increment = x -> x + 1;
        assertThat(ResettableValue.<Integer>undefined().mapAndGet(increment), nullValue());
        assertThat(ResettableValue.<Integer>reset().mapAndGet(increment), nullValue());
        int value = randomIntBetween(0, Integer.MAX_VALUE - 1);
        assertThat(ResettableValue.create(value).mapAndGet(increment), equalTo(value + 1));
    }

    public void testSerialisation() throws IOException {
        ResettableValue<Boolean> value = ResettableValue.create(randomBoolean());
        assertThat(writeAndRead(value), equalTo(value));

        assertThat(writeAndRead(ResettableValue.undefined()), equalTo(ResettableValue.undefined()));
        assertThat(writeAndRead(ResettableValue.reset()), equalTo(ResettableValue.reset()));
    }

    private ResettableValue<Boolean> writeAndRead(ResettableValue<Boolean> value) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            ResettableValue.write(output, value, StreamOutput::writeBoolean);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), new NamedWriteableRegistry(List.of()))) {
                return ResettableValue.read(in, StreamInput::readBoolean);
            }
        }
    }
}
