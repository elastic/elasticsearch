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
import java.util.Arrays;
import java.util.List;

public abstract class AbstractBWCSerializationTestCase<T extends Writeable & ToXContent> extends AbstractSerializingTestCase<T> {

    /**
     * Returns the expected instance if serialized from the given version.
     */
    protected abstract T mutateInstanceForVersion(T instance, Version version);

    /**
     * The bwc versions to test serialization against
     */
    protected List<Version> bwcVersions() {
        return Arrays.asList(Version.V_7_7_0);
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
        assertBwcEqualInstances(mutateInstanceForVersion(testInstance, version), deserializedInstance, version);
    }

    /**
     * Assert that two instances are equal.
     */
    protected final void assertBwcEqualInstances(T expectedInstance, T newInstance, Version version) {
        assertNotSame(version.toString(), newInstance, expectedInstance);
        assertEquals(version.toString(), expectedInstance, newInstance);
        assertEquals(version.toString(), expectedInstance.hashCode(), newInstance.hashCode());
    }

}
