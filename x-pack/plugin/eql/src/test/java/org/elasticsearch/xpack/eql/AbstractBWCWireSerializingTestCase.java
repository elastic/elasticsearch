/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.KnownTransportVersions.ALL_VERSIONS;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractBWCWireSerializingTestCase<T extends Writeable> extends AbstractWireSerializingTestCase<T> {

    private static List<TransportVersion> getAllBWCVersions() {
        int minCompatVersion = Collections.binarySearch(ALL_VERSIONS, TransportVersion.MINIMUM_COMPATIBLE);
        return ALL_VERSIONS.subList(minCompatVersion, ALL_VERSIONS.size());
    }

    private static final List<TransportVersion> DEFAULT_BWC_VERSIONS = getAllBWCVersions();

    protected abstract T mutateInstanceForVersion(T instance, TransportVersion version);

    public final void testBwcSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T testInstance = createTestInstance();
            for (TransportVersion bwcVersion : DEFAULT_BWC_VERSIONS) {
                assertBwcSerialization(testInstance, bwcVersion);
            }
        }
    }

    protected final void assertBwcSerialization(T testInstance, TransportVersion version) throws IOException {
        T deserializedInstance = copyInstance(testInstance, version);
        assertOnBWCObject(mutateInstanceForVersion(testInstance, version), deserializedInstance, version);
    }

    protected void assertOnBWCObject(T testInstance, T bwcDeserializedObject, TransportVersion version) {
        assertNotSame(version.toString(), bwcDeserializedObject, testInstance);
        assertThat(version.toString(), testInstance, equalTo(bwcDeserializedObject));
        assertEquals(version.toString(), testInstance.hashCode(), bwcDeserializedObject.hashCode());
    }
}
