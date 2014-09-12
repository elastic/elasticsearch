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

package org.elasticsearch.common.bytes;


import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Arrays;

public class BytesReferenceTests extends ElasticsearchTestCase {

    public void testEquals() {
        final int len = randomIntBetween(0, randomBoolean() ? 10: 100000);
        final int offset1 = randomInt(5);
        final byte[] array1 = new byte[offset1 + len + randomInt(5)];
        getRandom().nextBytes(array1);
        final int offset2 = randomInt(offset1);
        final byte[] array2 = Arrays.copyOfRange(array1, offset1 - offset2, array1.length);
        
        final BytesArray b1 = new BytesArray(array1, offset1, len);
        final BytesArray b2 = new BytesArray(array2, offset2, len);
        assertTrue(BytesReference.Helper.bytesEqual(b1, b2));
        assertTrue(BytesReference.Helper.bytesEquals(b1, b2));
        assertEquals(Arrays.hashCode(b1.toBytes()), b1.hashCode());
        assertEquals(BytesReference.Helper.bytesHashCode(b1), BytesReference.Helper.slowHashCode(b2));

        // test same instance
        assertTrue(BytesReference.Helper.bytesEqual(b1, b1));
        assertTrue(BytesReference.Helper.bytesEquals(b1, b1));
        assertEquals(BytesReference.Helper.bytesHashCode(b1), BytesReference.Helper.slowHashCode(b1));

        if (len > 0) {
            // test different length
            BytesArray differentLen = new BytesArray(array1, offset1, randomInt(len - 1));
            assertFalse(BytesReference.Helper.bytesEqual(b1, differentLen));

            // test changed bytes
            array1[offset1 + randomInt(len - 1)] += 13;
            assertFalse(BytesReference.Helper.bytesEqual(b1, b2));
            assertFalse(BytesReference.Helper.bytesEquals(b1, b2));
        }
    }

}
