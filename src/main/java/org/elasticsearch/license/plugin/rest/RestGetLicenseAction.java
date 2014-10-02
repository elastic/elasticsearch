/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.RepositoriesMetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.*;

import static org.elasticsearch.client.Requests.getRepositoryRequest;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestGetLicenseAction extends BaseRestHandler {

    @Inject
    public RestGetLicenseAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_cluster/license", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        final String[] repositories = request.paramAsStringArray("repository", Strings.EMPTY_ARRAY);
        //TODO: implement after custom metadata impl
        /*
        GetRepositoriesRequest getRepositoriesRequest = getRepositoryRequest(repositories);
        getRepositoriesRequest.masterNodeTimeout(request.paramAsTime("master_timeout", getRepositoriesRequest.masterNodeTimeout()));
        getRepositoriesRequest.local(request.paramAsBoolean("local", getRepositoriesRequest.local()));
        client.admin().cluster().getRepositories(getRepositoriesRequest, new RestBuilderListener<GetRepositoriesResponse>(channel) {
            @Override
            public RestResponse buildResponse(GetRepositoriesResponse response, XContentBuilder builder) throws Exception {
                builder.startObject();
                for (RepositoryMetaData repositoryMetaData : response.repositories()) {
                    RepositoriesMetaData.FACTORY.toXContent(repositoryMetaData, builder, request);
                }
                builder.endObject();

                return new BytesRestResponse(OK, builder);
            }
        });*/
    }
}
