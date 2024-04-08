/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.repositories.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Get repositories response
 */
public class GetRepositoriesResponse extends ActionResponse implements ToXContentObject {

    private final RepositoriesMetadata repositories;

    GetRepositoriesResponse(RepositoriesMetadata repositories) {
        this.repositories = repositories;
    }

    public GetRepositoriesResponse(StreamInput in) throws IOException {
        repositories = new RepositoriesMetadata(in);
    }

    /**
     * List of repositories to return
     *
     * @return list or repositories
     */
    public List<RepositoryMetadata> repositories() {
        return repositories.repositories();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        repositories.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        ChunkedToXContent.wrapAsToXContent(repositories)
            .toXContent(builder, new DelegatingMapParams(Map.of(RepositoriesMetadata.HIDE_GENERATIONS_PARAM, "true"), params));
        builder.endObject();
        return builder;
    }

}
