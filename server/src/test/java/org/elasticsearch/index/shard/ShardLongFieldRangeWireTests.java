/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

public class ShardLongFieldRangeWireTests extends AbstractWireSerializingTestCase<ShardLongFieldRange> {
    @Override
    protected Writeable.Reader<ShardLongFieldRange> instanceReader() {
        return ShardLongFieldRange::readFrom;
    }

    @Override
    protected ShardLongFieldRange createTestInstance() {
        return randomRange();
    }

    public static ShardLongFieldRange randomRange() {
        switch (between(1, 3)) {
            case 1:
                return ShardLongFieldRange.UNKNOWN;
            case 2:
                return ShardLongFieldRange.EMPTY;
            case 3:
                return randomSpecificRange();
            default:
                throw new AssertionError("impossible");
        }
    }

    static ShardLongFieldRange randomSpecificRange() {
        final long min = randomLong();
        return ShardLongFieldRange.of(min, randomLongBetween(min, Long.MAX_VALUE));
    }

    @Override
    protected ShardLongFieldRange mutateInstance(ShardLongFieldRange instance) throws IOException {
        if (instance == ShardLongFieldRange.UNKNOWN) {
            return randomBoolean() ? ShardLongFieldRange.EMPTY : randomSpecificRange();
        }
        if (instance == ShardLongFieldRange.EMPTY) {
            return randomBoolean() ? ShardLongFieldRange.UNKNOWN : randomSpecificRange();
        }

        switch (between(1, 4)) {
            case 1:
                return ShardLongFieldRange.UNKNOWN;
            case 2:
                return ShardLongFieldRange.EMPTY;
            case 3:
                return instance.getMin() == Long.MAX_VALUE
                        ? randomSpecificRange()
                        : ShardLongFieldRange.of(randomValueOtherThan(instance.getMin(),
                        () -> randomLongBetween(Long.MIN_VALUE, instance.getMax())), instance.getMax());
            case 4:
                return instance.getMax() == Long.MIN_VALUE
                        ? randomSpecificRange()
                        : ShardLongFieldRange.of(instance.getMin(), randomValueOtherThan(instance.getMax(),
                        () -> randomLongBetween(instance.getMin(), Long.MAX_VALUE)));
            default:
                throw new AssertionError("impossible");
        }
    }

    @Override
    protected void assertEqualInstances(ShardLongFieldRange expectedInstance, ShardLongFieldRange newInstance) {
        if (expectedInstance == ShardLongFieldRange.UNKNOWN || expectedInstance == ShardLongFieldRange.EMPTY) {
            assertSame(expectedInstance, newInstance);
        } else {
            super.assertEqualInstances(expectedInstance, newInstance);
        }
    }

}
