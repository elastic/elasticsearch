/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted.action;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.RestStatusToXContentListener;
import org.elasticsearch.xpack.core.repositories.encrypted.EncryptedRepositoryChangePasswordRequest;
import org.elasticsearch.xpack.core.repositories.encrypted.action.ChangeEncryptedRepositoryPasswordAction;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestEncryptedRepositoryChangePasswordAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "encrypted_repository_change_password_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_snapshot/{repository}/_change_password"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        EncryptedRepositoryChangePasswordRequest changePasswordRequest = new EncryptedRepositoryChangePasswordRequest();
        changePasswordRequest.repositoryName(request.param("repository"));
        try (XContentParser parser = request.contentParser()) {
            Map<String, String> requestAsMap = parser.mapStrings();
            changePasswordRequest.fromPasswordName(requestAsMap.get("from_password_name"));
            changePasswordRequest.toPasswordName(requestAsMap.get("to_password_name"));
        }
        return channel -> client.admin()
            .cluster()
            .execute(ChangeEncryptedRepositoryPasswordAction.INSTANCE, changePasswordRequest, new RestStatusToXContentListener<>(channel));
    }

}
