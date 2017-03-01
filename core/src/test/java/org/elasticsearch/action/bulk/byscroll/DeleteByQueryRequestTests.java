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

package org.elasticsearch.action.bulk.byscroll;

import org.elasticsearch.action.bulk.byscroll.AbstractBulkByScrollRequestTestCase;
import org.elasticsearch.action.bulk.byscroll.DeleteByQueryRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;

import static org.apache.lucene.util.TestUtil.randomSimpleString;

public class DeleteByQueryRequestTests extends AbstractBulkByScrollRequestTestCase<DeleteByQueryRequest> {
    public void testDeleteteByQueryRequestImplementsIndicesRequestReplaceable() {
        int numIndices = between(1, 100);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomSimpleString(random(), 1, 30);
        }

        SearchRequest searchRequest = new SearchRequest(indices);
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        searchRequest.indicesOptions(indicesOptions);

        DeleteByQueryRequest request = new DeleteByQueryRequest(searchRequest);
        for (int i = 0; i < numIndices; i++) {
            assertEquals(indices[i], request.indices()[i]);
        }

        assertSame(indicesOptions, request.indicesOptions());
        assertSame(request.indicesOptions(), request.getSearchRequest().indicesOptions());

        int numNewIndices = between(1, 100);
        String[] newIndices = new String[numNewIndices];
        for (int i = 0; i < numNewIndices; i++) {
            newIndices[i] = randomSimpleString(random(), 1, 30);
        }
        request.indices(newIndices);
        for (int i = 0; i < numNewIndices; i++) {;
            assertEquals(newIndices[i], request.indices()[i]);
        }
        for (int i = 0; i < numNewIndices; i++) {;
            assertEquals(newIndices[i], request.getSearchRequest().indices()[i]);
        }
    }

    @Override
    protected DeleteByQueryRequest newRequest() {
        return new DeleteByQueryRequest(new SearchRequest(randomAsciiOfLength(5)));
    }

    @Override
    protected void extraRandomizationForSlice(DeleteByQueryRequest original) {
        // Nothing else to randomize
    }

    @Override
    protected void extraForSliceAssertions(DeleteByQueryRequest original, DeleteByQueryRequest forSliced) {
        // No extra assertions needed
    }
}
