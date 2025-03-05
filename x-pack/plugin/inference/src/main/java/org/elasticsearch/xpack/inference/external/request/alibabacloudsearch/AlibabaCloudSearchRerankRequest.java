/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.alibabacloudsearch;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ByteArrayEntity;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.alibabacloudsearch.AlibabaCloudSearchAccount;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankModel;
import org.elasticsearch.xpack.inference.services.alibabacloudsearch.rerank.AlibabaCloudSearchRerankTaskSettings;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.buildUri;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;

public class AlibabaCloudSearchRerankRequest implements Request {
    private final AlibabaCloudSearchAccount account;
    private final String query;
    private final List<String> input;
    private final URI uri;
    private final AlibabaCloudSearchRerankTaskSettings taskSettings;
    private final String model;
    private final String host;
    private final String workspaceName;
    private final String httpSchema;
    private final String inferenceEntityId;

    public AlibabaCloudSearchRerankRequest(
        AlibabaCloudSearchAccount account,
        String query,
        List<String> input,
        AlibabaCloudSearchRerankModel rerankModel
    ) {
        Objects.requireNonNull(rerankModel);

        this.account = Objects.requireNonNull(account);
        this.query = Objects.requireNonNull(query);
        this.input = Objects.requireNonNull(input);
        taskSettings = rerankModel.getTaskSettings();
        model = rerankModel.getServiceSettings().getCommonSettings().modelId();
        host = rerankModel.getServiceSettings().getCommonSettings().getHost();
        workspaceName = rerankModel.getServiceSettings().getCommonSettings().getWorkspaceName();
        httpSchema = rerankModel.getServiceSettings().getCommonSettings().getHttpSchema() != null
            ? rerankModel.getServiceSettings().getCommonSettings().getHttpSchema()
            : "https";
        uri = buildUri(null, AlibabaCloudSearchUtils.SERVICE_NAME, this::buildDefaultUri);
        inferenceEntityId = rerankModel.getInferenceEntityId();
    }

    @Override
    public HttpRequest createHttpRequest() {
        HttpPost httpPost = new HttpPost(uri);

        ByteArrayEntity byteEntity = new ByteArrayEntity(
            Strings.toString(new AlibabaCloudSearchRerankRequestEntity(query, input, taskSettings)).getBytes(StandardCharsets.UTF_8)
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
                AlibabaCloudSearchUtils.RERANK_PATH,
                model
            )
            .build();
    }
}
