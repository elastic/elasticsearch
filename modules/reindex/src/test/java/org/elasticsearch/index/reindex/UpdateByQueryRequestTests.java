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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.apache.lucene.util.TestUtil.randomSimpleString;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class UpdateByQueryRequestTests extends ESTestCase {
    public void testUpdateByQueryRequestImplementsCompositeIndicesRequestWithDummies() {
        int numIndices = between(1, 100);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomSimpleString(random(), 1, 30);
        }
        UpdateByQueryRequest request = new UpdateByQueryRequest(new SearchRequest(indices));
        List<? extends IndicesRequest> subRequests = request.subRequests();
        assertThat(subRequests, hasSize(numIndices + 1));
        for (int i = 0; i < numIndices; i++) {
            assertThat(subRequests.get(i).indices(), arrayWithSize(1));
            assertEquals(indices[i], subRequests.get(i).indices()[0]);
        }
        assertThat(subRequests.get(numIndices), sameInstance(request.getSearchRequest()));
    }
}
