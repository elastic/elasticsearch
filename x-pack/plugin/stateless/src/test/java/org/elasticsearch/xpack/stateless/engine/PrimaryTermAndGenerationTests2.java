/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine;

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
