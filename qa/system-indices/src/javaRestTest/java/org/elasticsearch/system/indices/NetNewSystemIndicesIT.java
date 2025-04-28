
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.system.indices;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.junit.After;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class NetNewSystemIndicesIT extends AbstractSystemIndicesIT {

    public void testCreatingSystemIndex() throws Exception {
        ResponseException e = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("PUT", "/.net-new-system-index-primary"))
        );
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("system"));

        var request = new Request("PUT", "/_net_new_sys_index/_create");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testIndexDoc() throws Exception {
        String id = randomAlphaOfLength(4);

        ResponseException e = expectThrows(ResponseException.class, () -> {
            Request request = new Request("PUT", "/.net-new-system-index-primary/_doc" + id);
            request.setJsonEntity("{}");
            client().performRequest(request);
        });
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("system"));

        Request request = new Request("PUT", "/_net_new_sys_index/" + id);
        request.setJsonEntity("{}");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testSearch() throws Exception {
        // search before indexing doc
        Request searchRequest = new Request("GET", "/_search");
        searchRequest.setJsonEntity("{ \"query\": { \"match_all\": {} } }");
        searchRequest.addParameter("size", "10000");
        Response searchResponse = client().performRequest(searchRequest);
        assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(searchResponse.getEntity()), not(containsString(".net-new")));

        // create a doc
        String id = randomAlphaOfLength(4);
        Request request = new Request("PUT", "/_net_new_sys_index/" + id);
        request.setJsonEntity("{}");
        request.addParameter("refresh", "true");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "elastic"));
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // search again
        searchResponse = client().performRequest(searchRequest);
        assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(searchResponse.getEntity()), not(containsString(".net-new")));

        // index wildcard search
        searchRequest = new Request("GET", "/.net-new-system-index*/_search");
        searchRequest.setJsonEntity("{ \"query\": { \"match_all\": {} } }");
        searchRequest.addParameter("size", "10000");
        searchResponse = client().performRequest(searchRequest);
        assertThat(searchResponse.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(searchResponse.getEntity()), not(containsString(".net-new")));

        // direct index search
        Request directRequest = new Request("GET", "/.net-new-system-index-primary/_search");
        directRequest.setJsonEntity("{ \"query\": { \"match_all\": {} } }");
        directRequest.addParameter("size", "10000");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(directRequest));
        assertThat(EntityUtils.toString(e.getResponse().getEntity()), containsString("system"));
    }

    @After
    public void resetFeatures() throws Exception {
        client().performRequest(new Request("POST", "/_features/_reset"));
    }
}
