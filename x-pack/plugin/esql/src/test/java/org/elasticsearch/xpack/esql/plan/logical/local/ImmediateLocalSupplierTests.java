/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.compute.data.Block;

import static org.hamcrest.Matchers.equalTo;

public class ImmediateLocalSupplierTests extends LocalSupplierTests {

    @Override
    protected LocalSupplier createTestInstance() {
        Block[] blocks = randomList(1, 10, LocalSupplierTests::randomBlock).toArray(Block[]::new);
        return new ImmediateLocalSupplier(blocks);
    }

    protected void assertOnBWCObject(LocalSupplier testInstance, LocalSupplier bwcDeserializedObject, TransportVersion version) {
        assertNotSame(version.toString(), bwcDeserializedObject, testInstance);
        assertThat(version.toString(), testInstance, equalTo(bwcDeserializedObject));
        assertEquals(version.toString(), testInstance.hashCode(), bwcDeserializedObject.hashCode());
    }
}
