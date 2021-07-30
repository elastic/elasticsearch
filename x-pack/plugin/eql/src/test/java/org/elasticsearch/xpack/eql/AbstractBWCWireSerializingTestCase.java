/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.Version.getDeclaredVersions;
import static org.elasticsearch.xpack.eql.EqlTestUtils.EQL_GA_VERSION;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractBWCWireSerializingTestCase<T extends Writeable> extends AbstractWireSerializingTestCase<T> {

    private static final List<Version> ALL_VERSIONS = Collections.unmodifiableList(getDeclaredVersions(Version.class));

    private static List<Version> getAllBWCVersions(Version version) {
        return ALL_VERSIONS.stream().filter(v -> v.onOrAfter(EQL_GA_VERSION) && v.before(version) && version.isCompatible(v)).collect(
            Collectors.toList());
    }

    private static final List<Version> DEFAULT_BWC_VERSIONS = getAllBWCVersions(Version.CURRENT);

    protected abstract T mutateInstanceForVersion(T instance, Version version);

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
        assertOnBWCObject(mutateInstanceForVersion(testInstance, version), deserializedInstance, version);
    }

    protected void assertOnBWCObject(T testInstance, T bwcDeserializedObject, Version version) {
        assertNotSame(version.toString(), bwcDeserializedObject, testInstance);
        assertThat(version.toString(), testInstance, equalTo(bwcDeserializedObject));
        assertEquals(version.toString(), testInstance.hashCode(), bwcDeserializedObject.hashCode());
    }
}
