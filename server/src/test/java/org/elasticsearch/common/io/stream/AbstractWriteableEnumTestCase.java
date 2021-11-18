/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
