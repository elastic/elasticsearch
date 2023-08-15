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

import java.io.IOException;

public class PrimaryTermAndGenerationSerializationTests extends AbstractWireSerializingTestCase<PrimaryTermAndGeneration> {
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
        return new PrimaryTermAndGeneration(randomPositiveLong(), randomPositiveLong());
    }

    public static PrimaryTermAndGeneration mutatePrimaryTermAndGeneration(PrimaryTermAndGeneration instance) {
        return switch (randomInt(1)) {
            case 0 -> new PrimaryTermAndGeneration(
                randomValueOtherThan(instance.primaryTerm(), PrimaryTermAndGenerationSerializationTests::randomPositiveLong),
                instance.generation()
            );
            case 1 -> new PrimaryTermAndGeneration(
                instance.primaryTerm(),
                randomValueOtherThan(instance.generation(), PrimaryTermAndGenerationSerializationTests::randomPositiveLong)
            );
            default -> throw new IllegalArgumentException("Unexpected branch");
        };
    }

    private static long randomPositiveLong() {
        return randomLongBetween(0, Long.MAX_VALUE);
    }
}
