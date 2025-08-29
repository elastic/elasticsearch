/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class EmptyLocalSupplierTests extends LocalSupplierTests {

    @Override
    protected LocalSupplier createTestInstance() {
        return EmptyLocalSupplier.EMPTY;
    }

    protected void assertOnBWCObject(LocalSupplier testInstance, LocalSupplier bwcDeserializedObject, TransportVersion version) {
        assertSame(version.toString(), bwcDeserializedObject, testInstance);
        assertThat(version.toString(), bwcDeserializedObject, equalTo(EmptyLocalSupplier.EMPTY));
        assertEquals(version.toString(), testInstance.hashCode(), bwcDeserializedObject.hashCode());
    }

    @Override
    protected void writeTo(BytesStreamOutput output, LocalSupplier instance, TransportVersion version) throws IOException {
        if (version.onOrAfter(TransportVersions.ESQL_LOCAL_RELATION_WITH_NEW_BLOCKS)) {
            new PlanStreamOutput(output, null).writeNamedWriteable(instance);
        } else {
            output.writeVInt(0);
        }
    }
}
