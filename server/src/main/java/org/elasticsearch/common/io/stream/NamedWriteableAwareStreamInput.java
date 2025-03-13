/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    public <T extends NamedWriteable> List<T> readNamedWriteableCollectionAsList(Class<T> categoryClass) throws IOException {
        int count = readArraySize();
        if (count == 0) {
            return Collections.emptyList();
        }
        final Map<String, Writeable.Reader<?>> readers = namedWriteableRegistry.getReaders(categoryClass);
        List<T> builder = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String name = readString();
            builder.add(NamedWriteableRegistry.getReader(categoryClass, name, readers).read(this));
        }
        return builder;
    }

    @Override
    public <C extends NamedWriteable> C readNamedWriteable(
        @SuppressWarnings("unused") Class<C> categoryClass,
        @SuppressWarnings("unused") String name
    ) throws IOException {
        Writeable.Reader<? extends C> reader = namedWriteableRegistry.getReader(categoryClass, name);
        C c = reader.read(this);
        if (c == null) {
            throwOnNullRead(reader);
        }
        assert assertNameMatches(name, c);
        return c;
    }

    private static <C extends NamedWriteable> boolean assertNameMatches(String name, C c) {
        assert name.equals(c.getWriteableName())
            : c + " claims to have a different name [" + c.getWriteableName() + "] than it was read from [" + name + "].";
        return true;
    }

    @Override
    public NamedWriteableRegistry namedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
