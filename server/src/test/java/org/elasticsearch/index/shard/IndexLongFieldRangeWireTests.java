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
import java.util.Arrays;

import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.checkForSameInstances;
import static org.elasticsearch.index.shard.IndexLongFieldRangeTestUtils.randomRange;

public class IndexLongFieldRangeWireTests extends AbstractWireSerializingTestCase<IndexLongFieldRange> {
    @Override
    protected Writeable.Reader<IndexLongFieldRange> instanceReader() {
        return IndexLongFieldRange::readFrom;
    }

    @Override
    protected IndexLongFieldRange createTestInstance() {
        return randomRange();
    }

    @Override
    protected IndexLongFieldRange mutateInstance(IndexLongFieldRange instance) throws IOException {
        if (instance == IndexLongFieldRange.UNKNOWN) {
            return IndexLongFieldRangeTestUtils.randomSpecificRange();
        }

        if (randomBoolean()) {
            return IndexLongFieldRange.UNKNOWN;
        }

        while (true) {
            final IndexLongFieldRange newInstance = IndexLongFieldRangeTestUtils.randomSpecificRange();
            if (newInstance.getMinUnsafe() != instance.getMinUnsafe()
                    || newInstance.getMaxUnsafe() != instance.getMaxUnsafe()
                    || Arrays.equals(newInstance.getShards(), instance.getShards()) == false) {
                return newInstance;
            }
        }
    }


    @Override
    protected void assertEqualInstances(IndexLongFieldRange expectedInstance, IndexLongFieldRange newInstance) {
        if (checkForSameInstances(expectedInstance, newInstance) == false) {
            super.assertEqualInstances(expectedInstance, newInstance);
        }
    }

}
