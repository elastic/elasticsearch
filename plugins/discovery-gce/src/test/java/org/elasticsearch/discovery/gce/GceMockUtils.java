/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery.gce;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.Json;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

public class GceMockUtils {
    protected static final Logger logger = LogManager.getLogger(GceMockUtils.class);

    public static final String GCE_METADATA_URL = "http://metadata.google.internal/computeMetadata/v1/";

    protected static HttpTransport configureMock() {
        return new MockHttpTransport() {
            @Override
            public LowLevelHttpRequest buildRequest(String method, final String url) throws IOException {
                return new MockLowLevelHttpRequest() {
                    @Override
                    public LowLevelHttpResponse execute() throws IOException {
                        MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
                        response.setStatusCode(200);
                        response.setContentType(Json.MEDIA_TYPE);
                        if (url.startsWith(GCE_METADATA_URL)) {
                            logger.info("--> Simulate GCE Auth/Metadata response for [{}]", url);
                            response.setContent(readGoogleInternalJsonResponse(url));
                            response.addHeader("Metadata-Flavor", "Google");
                        } else {
                            logger.info("--> Simulate GCE API response for [{}]", url);
                            response.setContent(readGoogleApiJsonResponse(url));
                        }

                        return response;
                    }
                };
            }
        };
    }

    public static String readGoogleInternalJsonResponse(String url) throws IOException {
        return readJsonResponse(url, "http://metadata.google.internal/");
    }

    public static String readGoogleApiJsonResponse(String url) throws IOException {
        return readJsonResponse(url, "https://www.googleapis.com/");
    }

    private static String readJsonResponse(String url, String urlRoot) throws IOException {
        // We extract from the url the mock file path we want to use
        String mockFileName = Strings.replace(url, urlRoot, "");

        URL resource = GceMockUtils.class.getResource(mockFileName);
        if (resource == null) {
            throw new IOException("can't read [" + url + "] in src/test/resources/org/elasticsearch/discovery/gce");
        }
        try (InputStream is = FileSystemUtils.openFileURLStream(resource)) {
            final StringBuilder sb = new StringBuilder();
            Streams.readAllLines(is, sb::append);
            return sb.toString();
        }
    }
}
