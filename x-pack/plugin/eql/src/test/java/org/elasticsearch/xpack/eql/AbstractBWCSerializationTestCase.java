/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.Version.getDeclaredVersions;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractBWCSerializationTestCase<T extends Writeable & ToXContent> extends AbstractSerializingTestCase<T> {

    private static final List<Version> ALL_VERSIONS = Collections.unmodifiableList(getDeclaredVersions(Version.class));
    private static Version EQL_GA_VERSION = Version.V_7_10_0;

    private static List<Version> getAllBWCVersions(Version version) {
        return ALL_VERSIONS.stream().filter(v -> v.onOrAfter(EQL_GA_VERSION) && v.before(version) && version.isCompatible(v)).collect(
            Collectors.toList());
    }

    private static final List<Version> DEFAULT_BWC_VERSIONS = getAllBWCVersions(Version.CURRENT);

    public final void testBwcSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            T testInstance = createTestInstance();
            for (Version bwcVersion : DEFAULT_BWC_VERSIONS) {
                assertBwcSerialization(testInstance, bwcVersion);
            }
        }
    }

    protected final void assertBwcSerialization(T testInstance, Version version) throws IOException {
        T deserializedInstance = copyInstance(testInstance, version);
        assertOnBWCObject(testInstance, deserializedInstance, version);
    }

    protected void assertOnBWCObject(T testInstance, T bwcDeserializedObject, Version version) {
        assertNotSame(version.toString(), bwcDeserializedObject, testInstance);
        assertThat(version.toString(), testInstance, equalTo(bwcDeserializedObject));
        assertEquals(version.toString(), testInstance.hashCode(), bwcDeserializedObject.hashCode());
    }
}
