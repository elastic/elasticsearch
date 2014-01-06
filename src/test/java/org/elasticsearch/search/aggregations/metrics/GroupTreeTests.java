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

package org.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.collect.Lists;
import org.elasticsearch.search.aggregations.metrics.GroupTree.Group;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.GroupRedBlackTree;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.GroupRedBlackTree.SizeAndSum;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class GroupTreeTests extends ElasticsearchTestCase {

    public void testDuel() {
        GroupTree tree1 = new GroupTree();
        GroupRedBlackTree tree2 = new GroupRedBlackTree(randomInt(100));

        // Add elements
        final int elements = atLeast(100);
        for (int i = 0; i < elements; ++i) {
            final double centroid = randomDouble();
            final int count = randomIntBetween(1, 5);
            Group g = new Group(centroid, i);
            g.add(centroid, count - 1);
            tree1.add(g);
            tree2.addGroup(centroid, count, i);
        }
        assertEquals(tree1, tree2);

        // Remove
        List<Group> toRemove = Lists.newArrayList();
        for (Group group : tree1) {
            if (randomBoolean()) {
                toRemove.add(group);
            }
        }
        Collections.shuffle(toRemove, getRandom());
        for (Group group : toRemove) {
            tree1.remove(group);
            final boolean removed = tree2.removeGroup(group.mean(), group.id());
            assertTrue(removed);
        }
        assertEquals(tree1, tree2);
    }

    public static void assertEquals(GroupTree tree1, GroupRedBlackTree tree2) {
        assertEquals(tree1.size(), tree2.size());

        Iterator<Group> groups1 = tree1.iterator();
        Iterator<IntCursor> groups2 = tree2.iterator();
        while (true) {
            assertEquals(groups1.hasNext(), groups2.hasNext());
            if (!groups1.hasNext()) {
                break;
            }
            final Group next1 = groups1.next();
            final IntCursor next2 = groups2.next();
            assertEquals(next1.mean(), tree2.mean(next2.value), 0.0001);
            assertEquals(next1.count(), tree2.count(next2.value));
        }
        assertConsistent(tree2);
    }

    public static void assertConsistent(GroupRedBlackTree tree) {
        int i = 0;
        long sum = 0;
        for (IntCursor cursor : tree) {
            final int node = cursor.value;

            SizeAndSum s = new GroupRedBlackTree.SizeAndSum();
            tree.headSum(node, s);
            assertEquals(i, s.size);
            assertEquals(sum, s.sum);

            i++;
            sum += tree.count(node);
        }
    }
}
