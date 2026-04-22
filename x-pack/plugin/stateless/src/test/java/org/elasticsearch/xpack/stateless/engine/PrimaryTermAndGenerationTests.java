/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class PrimaryTermAndGenerationTests extends AbstractWireSerializingTestCase<PrimaryTermAndGeneration> {
    @Override
    protected Writeable.Reader<PrimaryTermAndGeneration> instanceReader() {
        return PrimaryTermAndGeneration::new;
    }

    @Override
    protected PrimaryTermAndGeneration createTestInstance() {
        return randomPrimaryTermAndGeneration();
    }

    @Override
    protected PrimaryTermAndGeneration mutateInstance(PrimaryTermAndGeneration instance) throws IOException {
        return mutatePrimaryTermAndGeneration(instance);
    }

    public static PrimaryTermAndGeneration randomPrimaryTermAndGeneration() {
        return new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonNegativeLong());
    }

    public static PrimaryTermAndGeneration mutatePrimaryTermAndGeneration(PrimaryTermAndGeneration instance) {
        return switch (randomInt(1)) {
            case 0 -> new PrimaryTermAndGeneration(
                randomValueOtherThan(instance.primaryTerm(), ESTestCase::randomNonNegativeLong),
                instance.generation()
            );
            case 1 -> new PrimaryTermAndGeneration(
                instance.primaryTerm(),
                randomValueOtherThan(instance.generation(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new IllegalArgumentException("Unexpected branch");
        };
    }

    public void testCompareTo() {
        var p1 = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonNegativeLong());
        var p2 = new PrimaryTermAndGeneration(p1.primaryTerm(), p1.generation());
        assertThat("p1=" + p1 + ", p2=" + p2, p1.compareTo(p2), equalTo(0));

        p1 = new PrimaryTermAndGeneration(randomNonNegativeLong(), randomNonNegativeInt());
        p2 = new PrimaryTermAndGeneration(p1.primaryTerm(), p1.generation() + randomLongBetween(1, Byte.MAX_VALUE));
        assertThat("p1=" + p1 + ", p2=" + p2, p1.compareTo(p2), lessThan(0));
        assertThat("p1=" + p1 + ", p2=" + p2, p2.compareTo(p1), greaterThan(0));

        p1 = new PrimaryTermAndGeneration(randomNonNegativeInt(), randomNonNegativeLong());
        p2 = new PrimaryTermAndGeneration(p1.primaryTerm() + randomLongBetween(1, Byte.MAX_VALUE), randomNonNegativeLong());
        assertThat("p1=" + p1 + ", p2=" + p2, p1.compareTo(p2), lessThan(0));
        assertThat("p1=" + p1 + ", p2=" + p2, p2.compareTo(p1), greaterThan(0));
    }
}
