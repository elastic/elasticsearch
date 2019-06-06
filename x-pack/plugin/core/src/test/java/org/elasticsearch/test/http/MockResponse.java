/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test.http;

import org.elasticsearch.common.unit.TimeValue;

/**
 * A response to be sent via the mock webserver. Parts of the response can be configured
 */
public class MockResponse {

    private String body = null;
    private int statusCode = 200;
    private TimeValue bodyDelay = null;
    private Headers headers = new Headers();
    private TimeValue beforeReplyDelay = null;

    /**
     * @param body The body to be returned if the response is sent by the webserver
     * @return The updated mock response
     */
    public MockResponse setBody(String body) {
        this.body = body;
        return this;
    }

    /**
     * @param statusCode The status code to be returned if the response is sent by the webserver, defaults to 200
     * @return The updated mock response
     */
    public MockResponse setResponseCode(int statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    /**
     * @param timeValue Allows to specify a delay between sending of headers and the body to inject artificial latency
     * @return The updated mock response
     */
    public MockResponse setBodyDelay(TimeValue timeValue) {
        this.bodyDelay = timeValue;
        return this;
    }

    /**
     * @param timeValue Allows to specify a delay before anything is sent back to the client
     * @return The updated mock response
     */
    public MockResponse setBeforeReplyDelay(TimeValue timeValue) {
        this.beforeReplyDelay = timeValue;
        return this;
    }

    /**
     * Adds a new header to a response
     * @param name Header name
     * @param value header value
     * @return The updated mock response
     */
    public MockResponse addHeader(String name, String value) {
        headers.add(name, value);
        return this;
    }

    /**
     * @return the body of the request
     */
    String getBody() {
        return body;
    }

    /**
     * @return The HTTP status code
     */
    int getStatusCode() {
        return statusCode;
    }

    /**
     * @return The time to delay the between sending the headers and the body
     */
    TimeValue getBodyDelay() {
        return bodyDelay;
    }

    /**
     * @return All configured headers for this request
     */
    Headers getHeaders() {
        return headers;
    }

    /**
     * @return The time to delay before the first byte is being returned
     */
    TimeValue getBeforeReplyDelay() {
        return beforeReplyDelay;
    }
}
