/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexFieldCapabilitiesWireTests extends ESTestCase {

    private static IndexFieldCapabilities runtimeField() {
        return new IndexFieldCapabilitiesBuilder("items.calc", "long").isRuntimeField(true).build();
    }

    private static IndexFieldCapabilities copy(IndexFieldCapabilities original, TransportVersion version) throws IOException {
        return copyWriteable(original, new NamedWriteableRegistry(java.util.List.of()), IndexFieldCapabilities::readFrom, version);
    }

    public void testRuntimeFieldFlagSurvivesCurrentVersion() throws IOException {
        IndexFieldCapabilities original = runtimeField();
        assertThat(copy(original, TransportVersion.current()), equalTo(original));
    }

    public void testRuntimeFieldFlagSurvivesEveryVersionThatSupportsIt() throws IOException {
        TransportVersion version = TransportVersionUtils.randomVersionSupporting(FieldCapabilities.FIELD_CAPS_RUNTIME_FIELD);
        IndexFieldCapabilities original = runtimeField();
        assertThat(copy(original, version).isRuntimeField(), is(true));
    }

    public void testRuntimeFieldFlagIsDroppedForVersionsThatDoNotSupportIt() throws IOException {
        TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(FieldCapabilities.FIELD_CAPS_RUNTIME_FIELD);
        IndexFieldCapabilities original = runtimeField();
        IndexFieldCapabilities copy = copy(original, version);
        assertThat(copy.isRuntimeField(), is(false));
        assertThat(copy.name(), equalTo(original.name()));
        assertThat(copy.type(), equalTo(original.type()));
    }

    public void testFieldThatIsNotARuntimeFieldStaysSoOnEveryVersion() throws IOException {
        IndexFieldCapabilities original = new IndexFieldCapabilitiesBuilder("items.qty", "long").build();
        assertThat(copy(original, TransportVersionUtils.randomCompatibleVersion()).isRuntimeField(), is(false));
    }
}
