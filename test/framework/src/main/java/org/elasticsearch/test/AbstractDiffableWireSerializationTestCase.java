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

import org.elasticsearch.cluster.Diff;
import org.elasticsearch.cluster.Diffable;
import org.elasticsearch.common.io.stream.Writeable.Reader;

import java.io.IOException;

/**
 * An abstract test case to ensure correct behavior of Diffable.
 *
 * This class can be used as a based class for tests of ClusterState.Custom classes and other classes that support,
 * Writable serialization and is diffable.
 */
public abstract class AbstractDiffableWireSerializationTestCase<T extends Diffable<T>> extends AbstractWireSerializingTestCase<T> {
    /**
     *  Introduces random changes into the test object
     */
    protected abstract T makeTestChanges(T testInstance);

    protected abstract Reader<Diff<T>> diffReader();

    public void testDiffableSerialization() throws IOException {
        DiffableTestUtils.testDiffableSerialization(this::createTestInstance, this::makeTestChanges, getNamedWriteableRegistry(),
            instanceReader(), diffReader());
    }

}
