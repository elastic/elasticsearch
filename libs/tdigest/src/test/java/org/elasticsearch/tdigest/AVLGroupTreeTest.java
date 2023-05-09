/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.tdigest;

import org.junit.Test;

public class AVLGroupTreeTest extends AbstractTest {

    @Test
    public void testSimpleAdds() {
        AVLGroupTree x = new AVLGroupTree(false);
        assertEquals(IntAVLTree.NIL, x.floor(34));
        assertEquals(IntAVLTree.NIL, x.first());
        assertEquals(IntAVLTree.NIL, x.last());
        assertEquals(0, x.size());
        assertEquals(0, x.sum());

        x.add(new Centroid(1));
        assertEquals(1, x.sum());
        Centroid centroid = new Centroid(2);
        centroid.add(3, 1);
        centroid.add(4, 1);
        x.add(centroid);

        assertEquals(2, x.size());
        assertEquals(4, x.sum());
    }

    @Test
    public void testBalancing() {
        AVLGroupTree x = new AVLGroupTree(false);
        for (int i = 0; i < 101; i++) {
            x.add(new Centroid(i));
        }

        assertEquals(101, x.size());
        assertEquals(101, x.sum());

        x.checkBalance();
        x.checkAggregates();
    }

    @Test
    public void testFloor() {
        // mostly tested in other tests
        AVLGroupTree x = new AVLGroupTree(false);
        for (int i = 0; i < 101; i++) {
            x.add(new Centroid(i / 2));
        }

        assertEquals(IntAVLTree.NIL, x.floor(-30));

        for (Centroid centroid : x) {
            assertEquals(centroid.mean(), x.mean(x.floor(centroid.mean() + 0.1)), 0);
        }
    }

    @Test
    public void testHeadSum() {
        AVLGroupTree x = new AVLGroupTree(false);
        for (int i = 0; i < 1000; ++i) {
            x.add(randomDouble(), randomIntBetween(1, 10), null);
        }
        long sum = 0;
        long last = -1;
        for (int node = x.first(); node != IntAVLTree.NIL; node = x.next(node)) {
            assertEquals(sum, x.headSum(node));
            sum += x.count(node);
            last = x.count(node);
        }
        assertEquals(last, x.count(x.last()));
    }

    @Test
    public void testFloorSum() {
        AVLGroupTree x = new AVLGroupTree(false);
        int total = 0;
        for (int i = 0; i < 1000; ++i) {
            int count = randomIntBetween(1, 10);
            x.add(randomDouble(), count, null);
            total += count;
        }
        assertEquals(IntAVLTree.NIL, x.floorSum(-1));
        for (long i = 0; i < total + 10; ++i) {
            final int floorNode = x.floorSum(i);
            assertTrue(x.headSum(floorNode) <= i);
            final int next = x.next(floorNode);
            assertTrue(next == IntAVLTree.NIL || x.headSum(next) > i);
        }
    }

}
