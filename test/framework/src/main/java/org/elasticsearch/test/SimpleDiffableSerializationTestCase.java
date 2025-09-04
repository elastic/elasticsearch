/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;

/**
 * An abstract test case to ensure correct behavior of Diffable.
 *
 * This class can be used as a based class for tests of Metadata.MetadataCustom classes and other classes that support,
 * Writable serialization, XContent-based serialization and is diffable.
 */
public abstract class SimpleDiffableSerializationTestCase<T extends Diffable<T> & ToXContent> extends AbstractXContentSerializingTestCase<
    T> {

    /**
     *  Introduces random changes into the test object
     */
    protected abstract T makeTestChanges(T testInstance);

    protected abstract Reader<Diff<T>> diffReader();

    public final void testDiffableSerialization() throws IOException {
        DiffableTestUtils.testDiffableSerialization(
            this::createTestInstance,
            this::makeTestChanges,
            getNamedWriteableRegistry(),
            instanceReader(),
            diffReader()
        );
    }
}
