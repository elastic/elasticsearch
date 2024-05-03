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
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Standard test case for testing the wire serialization of subclasses of {@linkplain NamedWriteable}.
 * See {@link AbstractWireSerializingTestCase} for subclasses of {@link Writeable}. While you *can*
 * use {@linkplain AbstractWireSerializingTestCase} to test susbclasses of {@linkplain NamedWriteable}
 * this superclass will also test reading and writing the name.
 */
public abstract class AbstractNamedWriteableTestCase<T extends NamedWriteable> extends AbstractWireTestCase<T> {
    // Force subclasses to override to customize the registry for their NamedWriteable
    @Override
    protected abstract NamedWriteableRegistry getNamedWriteableRegistry();

    /**
     * The type of {@link NamedWriteable} to read.
     */
    protected abstract Class<T> categoryClass();

    @Override
    protected T copyInstance(T instance, TransportVersion version) throws IOException {
        return copyNamedWriteable(instance, getNamedWriteableRegistry(), categoryClass(), version);
    }

}
