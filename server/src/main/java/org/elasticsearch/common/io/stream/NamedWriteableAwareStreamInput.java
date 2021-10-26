/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import java.io.IOException;

/**
 * Wraps a {@link StreamInput} and associates it with a {@link NamedWriteableRegistry}
 */
public class NamedWriteableAwareStreamInput extends FilterStreamInput {

    private final NamedWriteableRegistry namedWriteableRegistry;

    public NamedWriteableAwareStreamInput(StreamInput delegate, NamedWriteableRegistry namedWriteableRegistry) {
        super(delegate);
        this.namedWriteableRegistry = namedWriteableRegistry;
    }

    @Override
    public <C extends NamedWriteable> C readNamedWriteable(Class<C> categoryClass) throws IOException {
        String name = readString();
        return readNamedWriteable(categoryClass, name);
    }

    @Override
    public <C extends NamedWriteable> C readNamedWriteable(@SuppressWarnings("unused") Class<C> categoryClass,
                                                           @SuppressWarnings("unused") String name) throws IOException {
        Writeable.Reader<? extends C> reader = namedWriteableRegistry.getReader(categoryClass, name);
        C c = reader.read(this);
        if (c == null) {
            throw new IOException(
                "Writeable.Reader [" + reader + "] returned null which is not allowed and probably means it screwed up the stream.");
        }
        assert name.equals(c.getWriteableName()) : c + " claims to have a different name [" + c.getWriteableName()
            + "] than it was read from [" + name + "].";
        return c;
    }

    @Override
    public NamedWriteableRegistry namedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
