/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.termvectors;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

public class MultiTermVectorsRequestBuilder extends ActionRequestBuilder<MultiTermVectorsRequest, MultiTermVectorsResponse> {

    public MultiTermVectorsRequestBuilder(ElasticsearchClient client, MultiTermVectorsAction action) {
        super(client, action, new MultiTermVectorsRequest());
    }

    public MultiTermVectorsRequestBuilder add(String index, Iterable<String> ids) {
        for (String id : ids) {
            request.add(index, id);
        }
        return this;
    }

    public MultiTermVectorsRequestBuilder add(String index, String... ids) {
        for (String id : ids) {
            request.add(index, id);
        }
        return this;
    }

    public MultiTermVectorsRequestBuilder add(TermVectorsRequest termVectorsRequest) {
        request.add(termVectorsRequest);
        return this;
    }
}
