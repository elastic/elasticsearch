/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.test.rest.yaml;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Response obtained from a REST call, eagerly reads the response body into a string for later optional parsing.
 * Supports parsing the response body when needed and returning specific values extracted from it.
 */
public class ClientYamlTestResponse {

    private final Response response;
    private final byte[] body;
    private final XContentType bodyContentType;
    private ObjectPath parsedResponse;
    private String bodyAsString;

    public ClientYamlTestResponse(Response response) throws IOException {
        this.response = response;
        if (response.getEntity() != null) {
            String contentType = response.getHeader("Content-Type");

            this.bodyContentType = getContentTypeIgnoreExceptions(contentType);
            try {
                byte[] bytes = EntityUtils.toByteArray(response.getEntity());
                //skip parsing if we got text back (e.g. if we called _cat apis)
                if (bodyContentType != null) {
                    this.parsedResponse = ObjectPath.createFromXContent(bodyContentType.xContent(), new BytesArray(bytes));
                }
                this.body = bytes;
            } catch (IOException e) {
                EntityUtils.consumeQuietly(response.getEntity());
                throw e;
            }
        } else {
            this.body = null;
            this.bodyContentType = null;
        }
    }

    /**
     * A content type returned on a response can be a media type defined outside XContentType (for instance plain/text, plain/csv etc).
     * This means that the response cannot be parsed.DefaultHttpHeaders
     * Also in testing there is no access to media types defined outside of XContentType.
     * Therefore a null has to be returned if a response content-type has a mediatype not defined in XContentType.
     */
    private XContentType getContentTypeIgnoreExceptions(String contentType) {
        try {
            return XContentType.fromMediaType(contentType);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public int getStatusCode() {
        return response.getStatusLine().getStatusCode();
    }

    public String getReasonPhrase() {
        return response.getStatusLine().getReasonPhrase();
    }

    /**
     * Get a list of all of the values of all warning headers returned in the response.
     */
    public List<String> getWarningHeaders() {
        List<String> warningHeaders = new ArrayList<>();
        for (Header header : response.getHeaders()) {
            if (header.getName().equals("Warning")) {
                warningHeaders.add(header.getValue());
            }
        }
        return warningHeaders;
    }

    /**
     * Returns the body properly parsed depending on the content type.
     * Might be a string or a json object parsed as a map.
     */
    public Object getBody() throws IOException {
        if (parsedResponse != null) {
            return parsedResponse.evaluate("");
        }
        //we only get here if there is no response body or the body is text
        assert bodyContentType == null;
        return getBodyAsString();
    }

    /**
     * Returns the body as a string
     */
    public String getBodyAsString() {
        if (bodyAsString == null && body != null) {
            //content-type null means that text was returned
            if (bodyContentType == null || bodyContentType.canonical() == XContentType.JSON ||
                bodyContentType.canonical() == XContentType.YAML) {
                bodyAsString = new String(body, StandardCharsets.UTF_8);
            } else {
                //if the body is in a binary format and gets requested as a string (e.g. to log a test failure), we convert it to json
                try (XContentBuilder jsonBuilder = XContentFactory.jsonBuilder()) {
                    try (XContentParser parser = bodyContentType.xContent()
                            .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, body)) {
                        jsonBuilder.copyCurrentStructure(parser);
                    }
                    bodyAsString = Strings.toString(jsonBuilder);
                } catch (IOException e) {
                    throw new UncheckedIOException("unable to convert response body to a string format", e);
                }
            }
        }
        return bodyAsString;
    }

    public boolean isError() {
        return response.getStatusLine().getStatusCode() >= 400;
    }

    /**
     * Parses the response body and extracts a specific value from it (identified by the provided path)
     */
    public Object evaluate(String path) throws IOException {
        return evaluate(path, Stash.EMPTY);
    }

    /**
     * Parses the response body and extracts a specific value from it (identified by the provided path)
     */
    public Object evaluate(String path, Stash stash) throws IOException {
        if (response == null) {
            return null;
        }

        if (parsedResponse == null) {
            //special case: api that don't support body (e.g. exists) return true if 200, false if 404, even if no body
            //is_true: '' means the response had no body but the client returned true (caused by 200)
            //is_false: '' means the response had no body but the client returned false (caused by 404)
            if ("".equals(path) && HttpHead.METHOD_NAME.equals(response.getRequestLine().getMethod())) {
                return isError() == false;
            }
            return null;
        }

        return parsedResponse.evaluate(path, stash);
    }
}
