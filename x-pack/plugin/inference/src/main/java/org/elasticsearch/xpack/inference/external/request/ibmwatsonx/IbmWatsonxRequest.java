/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.ibmwatsonx;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.inference.external.request.Request;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public interface IbmWatsonxRequest extends Request {
    static void decorateWithBearerToken(HttpPost httpPost, DefaultSecretSettings secretSettings) {
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
                HttpEntity entity = response.getEntity();
                Map<String, Object> map;
                try (InputStream content = entity.getContent()) {
                    XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
                    map = XContentHelper.convertToMap(xContentType.xContent(), content, false);
                }
                return (String) map.get("access_token");
            });
        } catch (Exception e) {
            e.printStackTrace();
        }

        Header bearerHeader = new BasicHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken);
        httpPost.setHeader(bearerHeader);
    }
}
