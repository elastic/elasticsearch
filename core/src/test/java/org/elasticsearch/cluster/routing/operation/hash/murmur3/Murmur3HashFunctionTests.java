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

package org.elasticsearch.cluster.routing.operation.hash.murmur3;

import com.carrotsearch.randomizedtesting.generators.RandomInts;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.test.ElasticsearchTestCase;

public class Murmur3HashFunctionTests extends ElasticsearchTestCase {

    public void test() {
        // Make sure that we agree with guava
        Murmur3HashFunction murmur3 = new Murmur3HashFunction();
        HashFunction guavaMurmur3 = Hashing.murmur3_32();
        for (int i = 0; i < 100; ++i) {
            final String id = RandomStrings.randomRealisticUnicodeOfCodepointLength(getRandom(), RandomInts.randomIntBetween(getRandom(), 1, 20));
            //final String id = "0";
            final int hash1 = guavaMurmur3.newHasher().putUnencodedChars(id).hash().asInt();
            final int hash2 = murmur3.hash(id);
            assertEquals(hash1, hash2);
        }
    }

}
