/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
