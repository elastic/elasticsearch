/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.lucene.docset;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.OpenBitSet;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

@Test
public class SlicedOpenBitSetTests {

    @Test public void simpleTests() throws IOException {
        int numberOfBits = 500;
        SlicedOpenBitSet bitSet = new SlicedOpenBitSet(new long[OpenBitSet.bits2words(numberOfBits) + 100], OpenBitSet.bits2words(numberOfBits), 100);

        bitSet.fastSet(100);
        assertThat(bitSet.fastGet(100), equalTo(true));

        DocIdSetIterator iterator = bitSet.iterator();
        assertThat(iterator.nextDoc(), equalTo(100));
        assertThat(iterator.nextDoc(), equalTo(DocIdSetIterator.NO_MORE_DOCS));
    }

    @Test public void testCopy() throws IOException {
        int numberOfBits = 500;
        OpenBitSet bitSet = new OpenBitSet(numberOfBits);
        bitSet.set(100);

        SlicedOpenBitSet sBitSet = new SlicedOpenBitSet(new long[OpenBitSet.bits2words(numberOfBits) + 33], 33, bitSet);
        assertThat(sBitSet.get(100), equalTo(true));
    }
}