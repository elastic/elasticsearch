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
