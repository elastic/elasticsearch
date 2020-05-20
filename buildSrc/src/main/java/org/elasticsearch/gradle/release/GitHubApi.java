/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.release;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * This class encapsulates some of the cumbersome details of making calls to the GitHub v3 API.
 * It doesn't attempt to model the endpoints and payloads, it just makes it easy to perform the
 * HTTP calls, and automatically parses the JSON responses.
 *
 * @see <a href="https://developer.github.com/v3/">GitHub API v3</a>
 */
public class GitHubApi {
    private static final Logger LOGGER = Logging.getLogger(GitHubApi.class);
    private final ObjectMapper objectMapper;
    private final String accessToken;
    private final boolean simulate;

    public GitHubApi(boolean simulate) throws IOException {
        this.simulate = simulate;
        this.objectMapper = new ObjectMapper();
        this.accessToken = loadGitHubKey();
    }

    /**
     * Performs a GET to the specified URI. This method expects a <code>200 OK</code> response.
     * @return a parsed representation of the JSON response.
     */
    public JsonNode get(String uri) {
        LOGGER.debug("Sending GET request to {}", uri);

        try {
            HttpRequest request = makeRequest(uri).build();

            HttpResponse<InputStream> response = sendRequest(request, null, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() != 200) {
                throw new GradleException("Expect 200 OK for GET [" + response.uri() + "] but received [" + response.statusCode() + "]");
            }

            return objectMapper.readTree(response.body());
        } catch (Exception e) {
            throw new GradleException("GET request failed", e);
        }
    }

    /**
     * Performs a DELETE to the specified URI. This method expects a <code>200 OK</code> response. Any response payload is ignored.
     */
    public void delete(String uri) {
        LOGGER.debug("Sending DELETE request to {}", uri);

        try {
            HttpRequest request = makeRequest(uri).DELETE().build();

            HttpResponse<Void> response = sendRequest(request, null, HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() != 200) {
                throw new GradleException("Expect 200 OK for DELETE [" + response.uri() + "] but received [" + response.statusCode() + "]");
            }
        } catch (Exception e) {
            throw new GradleException("DELETE request failed", e);
        }
    }

    /**
     * Performs a POST to the specified URI. The supplied payload will be serialised to JSON and included in the request.
     * This method expects a <code>200 OK</code> response. Any response payload is ignored.
     */
    public void post(String uri, Object payload) {
        LOGGER.debug("Sending POST request to {}", uri);

        try {
            String serialisedPayload = objectMapper.writer().writeValueAsString(payload);

            HttpRequest request = makeRequest(uri).header("Content-Type", "application/json; charset=utf-8")
                .POST(HttpRequest.BodyPublishers.ofString(serialisedPayload))
                .build();

            HttpResponse<Void> response = sendRequest(request, serialisedPayload, HttpResponse.BodyHandlers.discarding());

            if (response.statusCode() != 200) {
                throw new GradleException("Expect 200 OK for POST [" + response.uri() + "] but received [" + response.statusCode() + "]");
            }
        } catch (Exception e) {
            throw new GradleException("POST request failed", e);
        }
    }

    /**
     * Builds a request object, adding the standard headers.
     */
    private HttpRequest.Builder makeRequest(String uri) {
        return HttpRequest.newBuilder()
            .uri(makeURI(uri))
            .header("Accept", "application/vnd.github.v3+json")
            .header("Authorization", "token " + this.accessToken);
    }

    /**
     * A wrapper around {@link HttpClient#send(HttpRequest, HttpResponse.BodyHandler)}. If {@link #simulate} is
     * <code>true</code>, only <code>GET</code> requests will be send, and all other requests will cause some
     * information to be logged but the request will not be sent.
     *
     * @param request the request to send
     * @param payload the payload, or <code>null</code>. This is required so that it can be included in the logged tracing information.
     * @param responseBodyHandler a handler for the response body
     */
    private <T> HttpResponse<T> sendRequest(HttpRequest request, String payload, HttpResponse.BodyHandler<T> responseBodyHandler)
        throws IOException, InterruptedException {
        if (simulate) {
            LOGGER.quiet(request.method() + " " + request.uri());
            if (payload != null) {
                LOGGER.quiet(payload);
            }

            if (request.method().equals("GET") == false) {
                return new StubHttpResponse<>();
            }
        }

        return HttpClient.newHttpClient().send(request, responseBodyHandler);
    }

    /**
     * Fetches the user's GitHub Personal Access token from disk, specifically from
     * <code>$HOME/.elastic/github_auth</code>
     */
    private String loadGitHubKey() throws IOException {
        final Path keyPath = Path.of(System.getenv("HOME"), ".elastic", "github_auth");
        LOGGER.debug("Attempting to load API key from {}", keyPath);

        if (Files.notExists(keyPath)) {
            throw new GradleException(
                "File ~/.elastic/github_auth doesn't exist. Generate a Personal Access Token at https://github.com/settings/applications"
            );
        }

        final String keyString = Files.readString(keyPath).trim();

        if (keyString.matches("^[0-9a-fA-F]{40}$") == false) {
            throw new GradleException("Invalid GitHub key: " + keyString);
        }

        return keyString;
    }

    /**
     * Constructs a URI, changing any checked exceptions into runtime exceptions.
     */
    private static URI makeURI(String uri) {
        try {
            return new URI(uri);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Used when {@link #simulate} is <code>true</code>.
     * @see #sendRequest(HttpRequest, String, BodyHandler)
     * @param <T>
     */
    private static class StubHttpResponse<T> implements HttpResponse<T> {
        @Override
        public int statusCode() {
            return 200;
        }

        @Override
        public HttpRequest request() {
            return null;
        }

        @Override
        public Optional<HttpResponse<T>> previousResponse() {
            return Optional.empty();
        }

        @Override
        public HttpHeaders headers() {
            return null;
        }

        @Override
        public T body() {
            return null;
        }

        @Override
        public Optional<SSLSession> sslSession() {
            return Optional.empty();
        }

        @Override
        public URI uri() {
            return null;
        }

        @Override
        public HttpClient.Version version() {
            return null;
        }
    }
}
