/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.completion;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.completion.AlibabaCloudSearchCompletionTaskSettings;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.AlibabaCloudSearchRequest;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.request.AlibabaCloudSearchUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class AlibabaCloudSearchCompletionRequest extends AlibabaCloudSearchRequest {
    private final AlibabaCloudSearchAccount account;
    private final List<String> input;
    private final URI uri;
    private final AlibabaCloudSearchCompletionTaskSettings taskSettings;
    private final String model;
    private final String host;
    private final String workspaceName;
    private final String httpSchema;
    private final String inferenceEntityId;

    public AlibabaCloudSearchCompletionRequest(
        AlibabaCloudSearchAccount account,
        List<String> input,
        AlibabaCloudSearchCompletionModel completionModel
    ) {
        Objects.requireNonNull(completionModel);

        this.account = Objects.requireNonNull(account);
        this.input = Objects.requireNonNull(input);
        taskSettings = completionModel.getTaskSettings();
        model = completionModel.getServiceSettings().getCommonSettings().modelId();
        host = completionModel.getServiceSettings().getCommonSettings().getHost();
        workspaceName = completionModel.getServiceSettings().getCommonSettings().getWorkspaceName();
        httpSchema = completionModel.getServiceSettings().getCommonSettings().getHttpSchema() != null
            ? completionModel.getServiceSettings().getCommonSettings().getHttpSchema()
            : "https";
        uri = buildUri(null, AlibabaCloudSearchUtils.SERVICE_NAME, this::buildDefaultUri);
        inferenceEntityId = completionModel.getInferenceEntityId();
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new AlibabaCloudSearchCompletionRequestEntity(input, taskSettings, model)).getBytes(StandardCharsets.UTF_8)
        );
        httpPost.setEntity(byteEntity);

        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, XContentType.JSON.mediaType());
        httpPost.setHeader(createAuthBearerHeader(account.apiKey()));

        return new HttpRequest(httpPost, getInferenceEntityId());
    }

    @Override
    public String getInferenceEntityId() {
        return inferenceEntityId;
    }

    @Override
    public URI getURI() {
        return uri;
    }

    @Override
    public Request truncate() {
        return this;
    }

    @Override
    public boolean[] getTruncationInfo() {
        return null;
    }

    URI buildDefaultUri() throws URISyntaxException {
        return new URIBuilder().setScheme(httpSchema)
            .setHost(host)
            .setPathSegments(
                AlibabaCloudSearchUtils.VERSION_3,
                AlibabaCloudSearchUtils.OPENAPI_PATH,
                AlibabaCloudSearchUtils.WORKSPACE_PATH,
                workspaceName,
                AlibabaCloudSearchUtils.COMPLETION_PATH,
                model
            )
            .build();
    }
}
