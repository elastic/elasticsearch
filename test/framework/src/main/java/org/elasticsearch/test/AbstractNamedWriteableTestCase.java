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

package org.elasticsearch.test;

import org.elasticsearch.Version;
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
    protected T copyInstance(T instance, Version version) throws IOException {
        return copyNamedWriteable(instance, getNamedWriteableRegistry(), categoryClass(), version);
    }

}
