/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * Abstract class offering base functionality for testing @{link Writeable} enums.
 */
public abstract class AbstractWriteableEnumTestCase extends ESTestCase {
    private final Writeable.Reader<?> reader;

    public AbstractWriteableEnumTestCase(Writeable.Reader<?> reader) {
        this.reader = reader;
    }

    /**
     * Test that the ordinals for the enum are consistent (i.e. the order hasn't changed)
     * because writing an enum to a stream often uses the ordinal value.
     */
    public abstract void testValidOrdinals();

    /**
     * Test that the conversion from a string to enum is correct.
     */
    public abstract void testFromString();

    /**
     * Test that the correct enum value is produced from the serialized value in the {@link StreamInput}.
     */
    public abstract void testReadFrom() throws IOException;

    /**
     * Test that the correct serialized value is produced from the {@link StreamOutput}.
     */
    public abstract void testWriteTo() throws IOException;

    // a convenience method for testing the write of a writeable enum
    protected static void assertWriteToStream(final Writeable writeableEnum, final int ordinal) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            writeableEnum.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(ordinal));
            }
        }
    }

    // a convenience method for testing the read of a writeable enum
    protected void assertReadFromStream(final int ordinal, final Writeable expected) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(ordinal);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(reader.read(in), equalTo(expected));
            }
        }
    }
}
