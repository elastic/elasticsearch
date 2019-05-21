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

package org.elasticsearch.client.core;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.client.core.TermVectorsRequest.createFromTemplate;

public class MultiTermVectorsRequest implements ToXContentObject, Validatable {

    private List<TermVectorsRequest> requests = new ArrayList<>();

    /**
     * Constructs an empty MultiTermVectorsRequest
     * After that use {@code add} method to add individual {@code TermVectorsRequest} to it.
     */
    public MultiTermVectorsRequest() {}

    /**
     * Constructs a MultiTermVectorsRequest from the given document ids
     * and a template {@code TermVectorsRequest}.
     * Used when individual requests share the same index, type and other settings.
     * @param ids - ids of documents for which term vectors are requested
     * @param template - a template {@code TermVectorsRequest} that allows to set all
     * settings only once for all requests.
     */
    public MultiTermVectorsRequest(String[] ids, TermVectorsRequest template) {
        for (String id : ids) {
            TermVectorsRequest request = createFromTemplate(template, id);
            requests.add(request);
        }
    }

    /**
     * Adds another {@code TermVectorsRequest} to this {@code MultiTermVectorsRequest}
     * @param request - {@code TermVectorsRequest} to add
     */
    public void add(TermVectorsRequest request) {
        requests.add(request);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("docs");
        for (TermVectorsRequest request : requests) {
            request.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
