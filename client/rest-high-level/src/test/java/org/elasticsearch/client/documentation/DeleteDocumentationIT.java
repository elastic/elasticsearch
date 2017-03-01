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

package org.elasticsearch.client.documentation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * This class is used to generate the Java Delete API documentation.
 * You need to wrap your code between two tags like:
 * // tag::example[]
 * // end::example[]
 *
 * Where example is your tag name.
 *
 * Then in the documentation, you can extract what is between tag and end tags with
 * ["source","java",subs="attributes,callouts"]
 * --------------------------------------------------
 * sys2::[perl -ne 'exit if /end::example/; print if $tag; $tag = $tag || /tag::example/' \
 *     {docdir}/../../client/rest-high-level/src/test/java/org/elasticsearch/client/documentation/DeleteDocumentationIT.java]
 * --------------------------------------------------
 */
public class DeleteDocumentationIT extends ESRestHighLevelClientTestCase {

    /**
     * This test documents docs/java-rest/high-level/document/delete.asciidoc
     */
    public void testDelete() throws IOException {
        RestHighLevelClient client = highLevelClient();

        // tag::delete-request[]
        DeleteRequest request = new DeleteRequest(
            "index",    // <1>
            "type",     // <2>
            "id");      // <3>
        // end::delete-request[]

        // tag::delete-request-props[]
        request.timeout(TimeValue.timeValueSeconds(1));                     // <1>
        request.timeout("1s");                                              // <2>
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);    // <3>
        request.setRefreshPolicy("wait_for");                               // <4>
        request.version(2);                                                 // <5>
        request.versionType(VersionType.EXTERNAL);                          // <6>
        // end::delete-request-props[]

        // tag::delete-execute[]
        DeleteResponse response = client.delete(request);
        // end::delete-execute[]

        try {
            // tag::delete-notfound[]
            if (response.getResult().equals(DocWriteResponse.Result.NOT_FOUND)) {
                throw new Exception("Can't find document to be removed"); // <1>
            }
            // end::delete-notfound[]
        } catch (Exception ignored) { }

        // tag::delete-execute-async[]
        client.deleteAsync(request, new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        });
        // end::delete-execute-async[]

        // tag::delete-conflict[]
        try {
            client.delete(request);
        } catch (ElasticsearchException exception) {
            if (exception.status().equals(RestStatus.CONFLICT)) {
                // <1>
            }
        }
        // end::delete-conflict[]

    }
}
