/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase.DEFAULT_BWC_VERSIONS;

public abstract class AbstractBWCSerializationTestCase<T extends Writeable & ToXContent> extends AbstractSerializingTestCase<T> {

    /**
     * Returns the expected instance if serialized from the given version.
     */
    protected abstract T mutateInstanceForVersion(T instance, Version version);

    /**
     * The bwc versions to test serialization against
     */
    protected List<Version> bwcVersions() {
        return DEFAULT_BWC_VERSIONS;
    }

    /**
     * Test serialization and deserialization of the test instance across versions
     */
    public final void testBwcSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T testInstance = createTestInstance();
            for (Version bwcVersion : bwcVersions()) {
                assertBwcSerialization(testInstance, bwcVersion);
            }
        }
    }

    /**
     * Assert that instances copied at a particular version are equal. The version is useful
     * for sanity checking the backwards compatibility of the wire. It isn't a substitute for
     * real backwards compatibility tests but it is *so* much faster.
     */
    protected final void assertBwcSerialization(T testInstance, Version version) throws IOException {
        T deserializedInstance = copyWriteable(testInstance, getNamedWriteableRegistry(), instanceReader(), version);
        assertOnBWCObject(deserializedInstance, mutateInstanceForVersion(testInstance, version), version);
    }

    /**
     * @param bwcSerializedObject The object deserialized from the previous version
     * @param testInstance The original test instance
     * @param version The version which serialized
     */
    protected void assertOnBWCObject(T bwcSerializedObject, T testInstance, Version version) {
        assertNotSame(version.toString(), bwcSerializedObject, testInstance);
        assertEquals(version.toString(), bwcSerializedObject, testInstance);
        assertEquals(version.toString(), bwcSerializedObject.hashCode(), testInstance.hashCode());
    }
}
