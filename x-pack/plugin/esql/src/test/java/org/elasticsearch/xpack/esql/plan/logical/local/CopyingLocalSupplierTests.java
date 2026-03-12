/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import static org.hamcrest.Matchers.equalTo;

public class CopyingLocalSupplierTests extends LocalSupplierTests {

    private static final TransportVersion ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS = TransportVersion.fromName(
        "esql_local_relation_with_new_blocks"
    );

    @Override
    protected LocalSupplier createTestInstance() {
        int blockSize = randomInt(1000);
        Block[] blocks = randomList(1, 10, () -> LocalSupplierTests.randomBlock(blockSize)).toArray(Block[]::new);
        return new CopyingLocalSupplier(new Page(blocks));
    }

    protected void assertOnBWCObject(LocalSupplier testInstance, LocalSupplier bwcDeserializedObject, TransportVersion version) {
        assertNotSame(version.toString(), bwcDeserializedObject, testInstance);
        if (version.supports(ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS)) {
            assertThat(testInstance, equalTo(bwcDeserializedObject));
        } else {
            assertTrue(version.toString(), bwcDeserializedObject instanceof ImmediateLocalSupplier);
        }
        assertEquals(version.toString(), testInstance.hashCode(), bwcDeserializedObject.hashCode());
    }
}
