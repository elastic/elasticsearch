/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.test.BWCVersions.DEFAULT_BWC_VERSIONS;
import static org.hamcrest.Matchers.is;

public abstract class AbstractBWCSerializationTestCase<T extends Writeable & ToXContent> extends AbstractXContentSerializingTestCase<T> {

    /**
     * Returns the expected instance if serialized from the given version.
     */
    protected abstract T mutateInstanceForVersion(T instance, TransportVersion version);

    /**
     * The bwc versions to test serialization against
     */
    protected Collection<TransportVersion> bwcVersions() {
        return DEFAULT_BWC_VERSIONS;
    }

    /**
     * Test serialization and deserialization of the test instance across versions
     */
    public final void testBwcSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T testInstance = createTestInstance();
            for (TransportVersion bwcVersion : bwcVersions()) {
                assertBwcSerialization(testInstance, bwcVersion);
            }
        }
    }

    /**
     * Assert that instances copied at a particular version are equal. The version is useful
     * for sanity checking the backwards compatibility of the wire. It isn't a substitute for
     * real backwards compatibility tests but it is *so* much faster.
     */
    protected final void assertBwcSerialization(T testInstance, TransportVersion version) throws IOException {
        T deserializedInstance = copyWriteable(testInstance, getNamedWriteableRegistry(), instanceReader(), version);
        assertOnBWCObject(deserializedInstance, mutateInstanceForVersion(testInstance, version), version);
    }

    /**
     * @param bwcSerializedObject The object deserialized from the previous version
     * @param testInstance The original test instance
     * @param version The version which serialized
     */
    protected void assertOnBWCObject(T bwcSerializedObject, T testInstance, TransportVersion version) {
        assertNotSame(version.toString(), bwcSerializedObject, testInstance);
        assertEquals(version.toString(), bwcSerializedObject, testInstance);
        assertEquals(version.toString(), bwcSerializedObject.hashCode(), testInstance.hashCode());
    }

    /**
     * Helper method to test cases where serialization to older versions is expected to fail. An example of such a case would be an existing
     * object adding a new Enum value which is not known to older versions and which has no equivalent default value that could be sent
     * instead. Any attempt to serialize the object will result in an exception on the older node when attempting to deserialize, so to
     * make the cause of the failure more obvious, an exception is thrown on the node doing the serialization.
     *
     * @param featureVersion the {@link TransportVersion} in which the change was introduced
     * @param expectFailureFunction a function which should return {@code true} if serializing the instance should be expected to fail
     * @param errorMessage the expected error message when serialization fails
     */
    protected void testSerializationIsNotBackwardsCompatible(
        TransportVersion featureVersion,
        Function<T, Boolean> expectFailureFunction,
        String errorMessage
    ) throws IOException {
        var unsupportedVersions = DEFAULT_BWC_VERSIONS.stream().filter(Predicate.not(version -> version.supports(featureVersion))).toList();
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var testInstance = createTestInstance();
            for (var unsupportedVersion : unsupportedVersions) {
                if (expectFailureFunction.apply(testInstance)) {
                    var statusException = assertThrows(
                        ElasticsearchStatusException.class,
                        () -> copyWriteable(testInstance, getNamedWriteableRegistry(), instanceReader(), unsupportedVersion)
                    );
                    assertThat(statusException.status(), is(RestStatus.BAD_REQUEST));
                    assertThat(statusException.getMessage(), is(errorMessage));
                } else {
                    // If the instance shouldn't expect serialization to fail, assert that it can still be serialized
                    assertBwcSerialization(testInstance, unsupportedVersion);
                }
            }
        }
    }
}
