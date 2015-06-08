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

package org.elasticsearch.benchmark.search.child;

import com.carrotsearch.hppc.ObjectArrayList;
import com.carrotsearch.hppc.ObjectHashSet;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;

import java.util.Random;

/**
 */
public class ParentChildIndexGenerator {

    private final static Random RANDOM = new Random();

    private final Client client;
    private final int numParents;
    private final int numChildrenPerParent;
    private final int queryValueRatio;

    public ParentChildIndexGenerator(Client client, int numParents, int numChildrenPerParent, int queryValueRatio) {
        this.client = client;
        this.numParents = numParents;
        this.numChildrenPerParent = numChildrenPerParent;
        this.queryValueRatio = queryValueRatio;
    }

    public void index() {
        // Memory intensive...
        ObjectHashSet<String> usedParentIds = new ObjectHashSet<>(numParents, 0.5d);
        ObjectArrayList<ParentDocument> parents = new ObjectArrayList<>(numParents);

        for (int i = 0; i < numParents; i++) {
            String parentId;
            do {
                parentId = RandomStrings.randomAsciiOfLength(RANDOM, 10);
            } while (!usedParentIds.add(parentId));
            String[] queryValues = new String[numChildrenPerParent];
            for (int j = 0; j < numChildrenPerParent; j++) {
                queryValues[j] = getQueryValue();
            }
            parents.add(new ParentDocument(parentId, queryValues));
        }

        int indexCounter = 0;
        int childIdCounter = 0;
        while (!parents.isEmpty()) {
            BulkRequestBuilder request = client.prepareBulk();
            for (int i = 0; !parents.isEmpty() && i < 100; i++) {
                int index = RANDOM.nextInt(parents.size());
                ParentDocument parentDocument = parents.get(index);

                if (parentDocument.indexCounter == -1) {
                    request.add(Requests.indexRequest("test").type("parent")
                            .id(parentDocument.parentId)
                            .source("field1", getQueryValue()));
                } else {
                    request.add(Requests.indexRequest("test").type("child")
                            .parent(parentDocument.parentId)
                            .id(String.valueOf(++childIdCounter))
                            .source("field2", parentDocument.queryValues[parentDocument.indexCounter]));
                }

                if (++parentDocument.indexCounter == parentDocument.queryValues.length) {
                    parents.remove(index);
                }
            }

            BulkResponse response = request.execute().actionGet();
            if (response.hasFailures()) {
                System.err.println("--> failures...");
            }

            indexCounter += response.getItems().length;
            if (indexCounter % 100000 == 0) {
                System.out.println("--> Indexed " + indexCounter + " documents");
            }
        }
    }

    public String getQueryValue() {
        return "value" + RANDOM.nextInt(numChildrenPerParent / queryValueRatio);
    }

    class ParentDocument {

        final String parentId;
        final String[] queryValues;
        int indexCounter;

        ParentDocument(String parentId, String[] queryValues) {
            this.parentId = parentId;
            this.queryValues = queryValues;
            this.indexCounter = -1;
        }
    }

}
