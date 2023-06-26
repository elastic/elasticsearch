/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.ChunkedToXContent;

import java.io.IOException;

/**
 * An abstract test case to ensure correct behavior of Diffable.
 *
 * This class can be used as a based class for tests of {@link org.elasticsearch.cluster.ClusterState.Custom} classes and other classes
 * that support, Writable serialization, chunked XContent-based serialization and are diffable.
 */
public abstract class ChunkedToXContentDiffableSerializationTestCase<T extends Diffable<T> & ChunkedToXContent> extends
    AbstractChunkedSerializingTestCase<T> {

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
