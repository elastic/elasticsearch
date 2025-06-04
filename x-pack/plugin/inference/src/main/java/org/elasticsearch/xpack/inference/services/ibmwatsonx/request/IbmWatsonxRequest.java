/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ibmwatsonx.request;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.external.http.retry.RetryException;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.AUTHENTICATION;
import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.BAD_REQUEST;
import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.PERMISSION_DENIED;
import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.REDIRECTION;
import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.SERVER_ERROR;
import static org.elasticsearch.xpack.inference.external.http.retry.BaseResponseHandler.UNSUCCESSFUL;

public interface IbmWatsonxRequest extends Request {

    static void decorateWithBearerToken(HttpPost httpPost, DefaultSecretSettings secretSettings, String inferenceId) {
        final Logger logger = LogManager.getLogger(IbmWatsonxRequest.class);
        String bearerTokenGenUrl = "https://iam.cloud.ibm.com/identity/token";
        String bearerToken = "";

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPost httpPostForBearerToken = new HttpPost(bearerTokenGenUrl);

            String body = "grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey=" + secretSettings.apiKey().toString();
            ByteArrayEntity byteEntity = new ByteArrayEntity(body.getBytes(StandardCharsets.UTF_8));

            httpPostForBearerToken.setEntity(byteEntity);
            httpPostForBearerToken.setHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded");

            bearerToken = SocketAccess.doPrivileged(() -> {
                HttpResponse response = httpClient.execute(httpPostForBearerToken);
                validateResponse(bearerTokenGenUrl, inferenceId, response);
                HttpEntity entity = response.getEntity();
                Map<String, Object> map;
                try (InputStream content = entity.getContent()) {
                    XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
                    map = XContentHelper.convertToMap(xContentType.xContent(), content, false);
                }
                return (String) map.get("access_token");
            });
        } catch (IOException e) {
            throw new XContentParseException("Failed to add Bearer token to the request");
        }

        Header bearerHeader = new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken);
        httpPost.setHeader(bearerHeader);
    }

    static void validateResponse(String bearerTokenGenUrl, String inferenceId, HttpResponse response) {
        int statusCode = response.getStatusLine().getStatusCode();
        if (RestStatus.isSuccessful(statusCode)) {
            return;
        }

        if (statusCode == 500) {
            throw new RetryException(true, buildError(SERVER_ERROR, inferenceId, response));
        } else if (statusCode == 404) {
            throw new RetryException(false, buildError(resourceNotFoundError(bearerTokenGenUrl), inferenceId, response));
        } else if (statusCode == 403) {
            throw new RetryException(false, buildError(PERMISSION_DENIED, inferenceId, response));
        } else if (statusCode == 401) {
            throw new RetryException(false, buildError(AUTHENTICATION, inferenceId, response));
        } else if (statusCode == 400) {
            throw new RetryException(false, buildError(BAD_REQUEST, inferenceId, response));
        } else if (statusCode >= 300 && statusCode < 400) {
            throw new RetryException(false, buildError(REDIRECTION, inferenceId, response));
        } else {
            throw new RetryException(false, buildError(UNSUCCESSFUL, inferenceId, response));
        }
    }

    private static String resourceNotFoundError(String bearerTokenGenUrl) {
        return format("Resource not found at [%s]", bearerTokenGenUrl);
    }

    private static Exception buildError(String message, String inferenceId, HttpResponse response) {
        var errorMsg = response.getStatusLine().getReasonPhrase();
        var responseStatusCode = response.getStatusLine().getStatusCode();

        if (errorMsg == null) {
            return new ElasticsearchStatusException(
                format(
                    "%s for request to generate Bearer Token from inference entity id [%s] status [%s]",
                    message,
                    inferenceId,
                    responseStatusCode
                ),
                toRestStatus(responseStatusCode)
            );
        }

        return new ElasticsearchStatusException(
            format(
                "%s for request to generate Bearer Token from inference entity id [%s] status [%s]. Error message: [%s]",
                message,
                inferenceId,
                responseStatusCode,
                errorMsg
            ),
            toRestStatus(responseStatusCode)
        );
    }

    private static RestStatus toRestStatus(int statusCode) {
        RestStatus code = null;
        if (statusCode < 500) {
            code = RestStatus.fromCode(statusCode);
        }

        return code == null ? RestStatus.BAD_REQUEST : code;
    }
}
