/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;
import java.util.HexFormat;

import static org.elasticsearch.cluster.metadata.ProjectId.isValidFormatId;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ProjectIdTests extends AbstractWireSerializingTestCase<ProjectId> {

    public void testCannotCreateBlankProjectId() {
        expectThrows(IllegalArgumentException.class, () -> ProjectId.fromId(null));
        expectThrows(IllegalArgumentException.class, () -> ProjectId.fromId(""));
        expectThrows(IllegalArgumentException.class, () -> ProjectId.fromId(" "));
    }

    public void testValidateProjectId() {
        assertThat(isValidFormatId("🥸"), is(false));
        assertThat(isValidFormatId("Ă"), is(false));
        assertThat(isValidFormatId("Ø"), is(false));
        assertThat(isValidFormatId("õ"), is(false));
        assertThat(isValidFormatId("0️⃣"), is(false));
        assertThat(isValidFormatId("ア"), is(false));
        assertThat(isValidFormatId(" "), is(false));
        assertThat(isValidFormatId("\n"), is(false));
        assertThat(isValidFormatId("\t"), is(false));
        assertThat(isValidFormatId("\na"), is(false));
        assertThat(isValidFormatId("z "), is(false));
        assertThat(isValidFormatId("."), is(false));
        assertThat(isValidFormatId("@abc"), is(false));
        assertThat(isValidFormatId("1+2"), is(false));
        assertThat(isValidFormatId("xyz#"), is(false));
        assertThat(isValidFormatId("<qwerty>"), is(false));
        assertThat(isValidFormatId("x".repeat(129)), is(false));

        assertThat(isValidFormatId("a"), is(true));
        assertThat(isValidFormatId("1"), is(true));
        assertThat(isValidFormatId("valid-project-id"), is(true));
        assertThat(isValidFormatId("qwertyuiop_asdfghjkl_zxcvbnm"), is(true));
        assertThat(isValidFormatId("192020"), is(true));
        assertThat(isValidFormatId(randomUUID()), is(true));
        assertThat(isValidFormatId(HexFormat.of().formatHex(randomByteArrayOfLength(18))), is(true));
    }

    @Override
    protected Writeable.Reader<ProjectId> instanceReader() {
        return ProjectId.READER;
    }

    @Override
    protected ProjectId createTestInstance() {
        return switch (randomIntBetween(1, 4)) {
            case 1 -> randomUniqueProjectId();
            case 2 -> ProjectId.fromId(randomAlphaOfLengthBetween(1, 30));
            case 3 -> ProjectId.fromId(Long.toString(randomLongBetween(1, Long.MAX_VALUE)));
            default -> ProjectId.fromId(Long.toString(randomLongBetween(1, Long.MAX_VALUE), Character.MAX_RADIX));
        };
    }

    @Override
    protected ProjectId mutateInstance(ProjectId instance) throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    public void testToString() {
        String s = randomAlphaOfLengthBetween(8, 16);
        ProjectId id = ProjectId.fromId(s);
        assertThat(id.toString(), equalTo(s));
    }
}
