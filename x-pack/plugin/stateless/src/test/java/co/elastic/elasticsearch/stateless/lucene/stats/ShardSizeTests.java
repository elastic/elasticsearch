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

package co.elastic.elasticsearch.stateless.lucene.stats;

import co.elastic.elasticsearch.stateless.api.ShardSizeStatsReader.ShardSize;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class ShardSizeTests extends AbstractWireSerializingTestCase<ShardSize> {

    @Override
    protected Writeable.Reader<ShardSize> instanceReader() {
        return ShardSize::from;
    }

    @Override
    protected ShardSize createTestInstance() {
        return randomShardSize();
    }

    @Override
    protected ShardSize mutateInstance(ShardSize instance) {
        return switch (randomInt(2)) {
            case 0 -> new ShardSize(
                randomValueOtherThan(instance.interactiveSizeInBytes(), ESTestCase::randomNonNegativeLong),
                instance.nonInteractiveSizeInBytes(),
                instance.primaryTerm(),
                instance.generation()
            );
            case 1 -> new ShardSize(
                instance.interactiveSizeInBytes(),
                randomValueOtherThan(instance.nonInteractiveSizeInBytes(), ESTestCase::randomNonNegativeLong),
                instance.primaryTerm(),
                instance.generation()
            );
            case 2 -> new ShardSize(
                instance.interactiveSizeInBytes(),
                instance.nonInteractiveSizeInBytes(),
                randomValueOtherThan(instance.primaryTerm(), ESTestCase::randomNonNegativeLong),
                randomValueOtherThan(instance.primaryTerm(), ESTestCase::randomNonNegativeLong)
            );
            default -> randomValueOtherThan(instance, ShardSizeTests::randomShardSize);
        };
    }

    public static ShardSize randomShardSize() {
        return new ShardSize(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    public final void testBackwardsCompatibleSerialization() throws IOException {
        // reader prior to unnesting PrimaryTermAndGeneration into ShardSize
        Writeable.Reader<ShardSize> oldInstanceReader = in -> {
            var interactiveSizeInBytes = in.readLong();
            var nonInteractiveSizeInBytes = in.readLong();
            var primaryTermAndGeneration = new PrimaryTermAndGeneration(in);
            return new ShardSize(
                interactiveSizeInBytes,
                nonInteractiveSizeInBytes,
                primaryTermAndGeneration.primaryTerm(),
                primaryTermAndGeneration.generation()
            );
        };

        // writer prior to unnesting PrimaryTermAndGeneration into ShardSize
        Writeable.Writer<ShardSize> oldInstanceWriter = (out, v) -> {
            out.writeLong(v.interactiveSizeInBytes());
            out.writeLong(v.nonInteractiveSizeInBytes());
            new PrimaryTermAndGeneration(v.primaryTerm(), v.generation()).writeTo(out);
        };

        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var instance = createTestInstance();
            assertEqualInstances(instance, copyInstance(instance, instanceWriter(), oldInstanceReader));
            assertEqualInstances(instance, copyInstance(instance, oldInstanceWriter, instanceReader()));
        }
    }

    public final void testOnOrBefore() {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            var instance1 = createTestInstance();
            var primaryTermAndGeneration1 = new PrimaryTermAndGeneration(instance1.primaryTerm(), instance1.generation());

            var instance2 = createTestInstance();
            var primaryTermAndGeneration2 = new PrimaryTermAndGeneration(instance2.primaryTerm(), instance2.generation());

            assertThat(instance1.onOrBefore(instance2), is(primaryTermAndGeneration1.compareTo(primaryTermAndGeneration2) <= 0));
            assertThat(instance1.onOrBefore(instance2), is(primaryTermAndGeneration1.onOrBefore(primaryTermAndGeneration2)));
        }
    }

    private ShardSize copyInstance(ShardSize original, Writeable.Writer<ShardSize> writer, Writeable.Reader<ShardSize> reader)
        throws IOException {
        return copyInstance(original, getNamedWriteableRegistry(), writer, reader, TransportVersion.current());
    }
}
