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

package org.elasticsearch.kibana;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class KibanaSystemIndexIT extends ESIntegTestCase {

    public void testBulkToKibanaIndex() throws IOException {
        Request request = new Request("POST", "/_kibana/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request invalidIndexRequest = new Request("POST", "/_kibana/_bulk");
        invalidIndexRequest.setJsonEntity("{ \"index\" : { \"_index\" : \"test\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(invalidIndexRequest));
        response = e.getResponse();
        assertThat(response.getStatusLine().getStatusCode(), is(400));
        assertThat(EntityUtils.toString(response.getEntity()), containsString("does not fall within the set"));
    }
}
