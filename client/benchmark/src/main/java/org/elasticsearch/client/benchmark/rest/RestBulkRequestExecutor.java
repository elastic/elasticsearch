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
package org.elasticsearch.client.benchmark.rest;

import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.benchmark.ops.bulk.BulkRequestExecutor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class RestBulkRequestExecutor implements BulkRequestExecutor {
    private final RestClient client;
    private final String actionAndMetaData;

    public RestBulkRequestExecutor(RestClient client, String index, String type) {
        this.client = client;
        this.actionAndMetaData = String.format(Locale.ROOT, "{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }%n", index, type);
    }

    @Override
    public boolean bulkIndex(List<String> bulkData) {
        StringBuilder bulkRequestBody = new StringBuilder();
        for (String bulkItem : bulkData) {
            bulkRequestBody.append(actionAndMetaData);
            bulkRequestBody.append(bulkItem);
            bulkRequestBody.append("\n");
        }
        StringEntity entity = new StringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
        Response response = null;
        try {
            response = client.performRequest("POST", "/_bulk", Collections.emptyMap(), entity);
            //TODO dm: we could/should analyze the response contents to determine whether a bulk has failed
            return response.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED;
        } catch (Exception e) {
            throw new ElasticsearchException(e);
        } finally {
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }
}
