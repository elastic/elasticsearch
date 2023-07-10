/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;

/**
 * Standard test case for testing the wire serialization of subclasses of {@linkplain Writeable}.
 * See {@link AbstractNamedWriteableTestCase} for subclasses of {@link NamedWriteable}.
 * See {@link AbstractSerializationTestCase} for subclasses of {@link ToXContent} or {@link ChunkedToXContent}
 */
public abstract class AbstractWireSerializingTestCase<T extends Writeable> extends AbstractWireTestCase<T> {
    /**
     * Returns a {@link Writeable.Reader} that can be used to de-serialize the instance
     */
    protected abstract Writeable.Reader<T> instanceReader();

    /**
     * Copy the {@link Writeable} by round tripping it through {@linkplain StreamInput} and {@linkplain StreamOutput}.
     */
    @Override
    protected final T copyInstance(T instance, TransportVersion version) throws IOException {
        return copyWriteable(instance, getNamedWriteableRegistry(), instanceReader(), version);
    }
}
