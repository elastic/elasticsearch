/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        Symbol symbol = readSymbol();
        return readNamedWriteable(categoryClass, symbol);
    }

    @Override
    public <T extends NamedWriteable> List<T> readNamedWriteableCollectionAsList(Class<T> categoryClass) throws IOException {
        int count = readArraySize();
        if (count == 0) {
            return Collections.emptyList();
        }
        final Map<Symbol, Writeable.Reader<?>> readers = namedWriteableRegistry.getReaders(categoryClass);
        List<T> builder = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            Symbol symbol = readSymbol();
            T namedWriteable = NamedWriteableRegistry.getReader(categoryClass, symbol, readers).read(this);

            assert assertSymbolMatches(symbol, namedWriteable);
            builder.add(namedWriteable);
        }
        return builder;
    }

    @Override
    public <C extends NamedWriteable> C readNamedWriteable(Class<C> categoryClass, Symbol symbol) throws IOException {
        Writeable.Reader<? extends C> reader = namedWriteableRegistry.getReader(categoryClass, symbol);
        C namedWritable = reader.read(this);
        if (namedWritable == null) {
            throwOnNullRead(reader);
        }
        assert assertSymbolMatches(symbol, namedWritable);
        return namedWritable;
    }

    private static <C extends NamedWriteable> boolean assertSymbolMatches(Symbol symbol, C c) {
        assert symbol.equals(c.getNameSymbol())
            : c + " claims to have a different symbol [" + c.getNameSymbol() + "] than it was read from [" + symbol + "].";

        return true;
    }

    @Override
    public NamedWriteableRegistry namedWriteableRegistry() {
        return namedWriteableRegistry;
    }
}
