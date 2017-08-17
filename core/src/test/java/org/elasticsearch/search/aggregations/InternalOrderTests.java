/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.InternalOrder.CompoundOrder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InternalOrderTests extends AbstractSerializingTestCase<BucketOrder> {

    @Override
    protected BucketOrder createTestInstance() {
        if (randomBoolean()) {
            return getRandomOrder();
        } else {
            List<BucketOrder> orders = new ArrayList<>();
            for (int i = 0; i < randomInt(3); i++) {
                orders.add(getRandomOrder());
            }
            return BucketOrder.compound(orders);
        }
    }

    private BucketOrder getRandomOrder() {
        switch(randomInt(2)) {
            case 0: return BucketOrder.key(randomBoolean());
            case 1: return BucketOrder.count(randomBoolean());
            default: return BucketOrder.aggregation(randomAlphaOfLength(10), randomBoolean());
        }
    }

    @Override
    protected Reader<BucketOrder> instanceReader() {
        return InternalOrder.Streams::readOrder;
    }

    @Override
    protected BucketOrder doParseInstance(XContentParser parser) throws IOException {
        Token token = parser.nextToken();
        if (token == Token.START_OBJECT) {
            return InternalOrder.Parser.parseOrderParam(parser);
        }
        if (token == Token.START_ARRAY) {
            List<BucketOrder> orders = new ArrayList<>();
            while (parser.nextToken() == Token.START_OBJECT) {
                orders.add(InternalOrder.Parser.parseOrderParam(parser));
            }
            return BucketOrder.compound(orders);
        }
        return null;
    }

    @Override
    protected BucketOrder assertSerialization(BucketOrder testInstance) throws IOException {
        // identical behavior to AbstractWireSerializingTestCase, except assertNotSame is only called for
        // compound and aggregation order because _key and _count orders are static instances.
        BucketOrder deserializedInstance = copyInstance(testInstance);
        assertEquals(testInstance, deserializedInstance);
        assertEquals(testInstance.hashCode(), deserializedInstance.hashCode());
        if(testInstance instanceof CompoundOrder || testInstance instanceof InternalOrder.Aggregation) {
            assertNotSame(testInstance, deserializedInstance);
        }
        return deserializedInstance;
    }

    @Override
    protected void assertParsedInstance(XContentType xContentType, BytesReference instanceAsBytes, BucketOrder expectedInstance)
        throws IOException {
        // identical behavior to AbstractSerializingTestCase, except assertNotSame is only called for
        // compound and aggregation order because _key and _count orders are static instances.
        XContentParser parser = createParser(XContentFactory.xContent(xContentType), instanceAsBytes);
        BucketOrder newInstance = parseInstance(parser);
        assertEquals(expectedInstance, newInstance);
        assertEquals(expectedInstance.hashCode(), newInstance.hashCode());
        if(expectedInstance instanceof CompoundOrder || expectedInstance instanceof InternalOrder.Aggregation) {
            assertNotSame(newInstance, expectedInstance);
        }
    }

    public void testHistogramOrderBwc() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TEST_RUNS; runs++) {
            BucketOrder order = createTestInstance();
            Version bwcVersion = VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(),
                VersionUtils.getPreviousVersion(Version.V_6_0_0_alpha2));
            boolean bwcOrderFlag = randomBoolean();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(bwcVersion);
                InternalOrder.Streams.writeHistogramOrder(order, out, bwcOrderFlag);
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(bwcVersion);
                    BucketOrder actual = InternalOrder.Streams.readHistogramOrder(in, bwcOrderFlag);
                    BucketOrder expected = order;
                    if (order instanceof CompoundOrder) {
                        expected = ((CompoundOrder) order).orderElements.get(0);
                    }
                    assertEquals(expected, actual);
                }
            }
        }
    }

    public void testAggregationOrderEqualsAndHashCode() {
        String path = randomAlphaOfLength(10);
        boolean asc = randomBoolean();
        BucketOrder o1 = BucketOrder.aggregation(path, asc);
        BucketOrder o2 = BucketOrder.aggregation(path + "test", asc);
        BucketOrder o3 = BucketOrder.aggregation(path, !asc);
        BucketOrder o4 = BucketOrder.aggregation(path, asc);
        assertNotEquals(o1, o2);
        assertNotEquals(o1.hashCode(), o2.hashCode());
        assertNotEquals(o1, o3);
        assertNotEquals(o1.hashCode(), o3.hashCode());
        assertEquals(o1, o4);
        assertEquals(o1.hashCode(), o4.hashCode());

        o1 = InternalOrder.compound(o1);
        o2 = InternalOrder.compound(o2);
        o3 = InternalOrder.compound(o3);
        assertNotEquals(o1, o2);
        assertNotEquals(o1.hashCode(), o2.hashCode());
        assertNotEquals(o1, o2);
        assertNotEquals(o1.hashCode(), o2.hashCode());
        assertNotEquals(o1, o3);
        assertNotEquals(o1.hashCode(), o3.hashCode());
        assertNotEquals(o1, o4);
        assertNotEquals(o1.hashCode(), o4.hashCode());
    }

    @Override
    protected BucketOrder mutateInstance(BucketOrder instance) throws IOException {
        if (instance == InternalOrder.KEY_ASC) {
            return InternalOrder.COUNT_ASC;
        } else if (instance == InternalOrder.KEY_DESC) {
            return InternalOrder.KEY_ASC;
        } else if (instance == InternalOrder.COUNT_ASC) {
            return BucketOrder.aggregation(randomAlphaOfLengthBetween(1, 20), randomBoolean());
        } else if (instance == InternalOrder.COUNT_DESC) {
            return BucketOrder.compound(getRandomOrder());
        } else if (instance instanceof InternalOrder.Aggregation) {
            return InternalOrder.COUNT_DESC;
        } else {
            return InternalOrder.KEY_DESC;
        }
    }

}
