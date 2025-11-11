/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.rest.RestRequest.Method.POST;

/**
 * REST action for manually adding documents to a sample.
 * <p>
 * Handles POST requests to /{index}/_sample/docs endpoint and delegates
 * to the PostSampleDocsAction transport action.
 * </p>
 *
 * <p>Example usage:</p>
 * <pre>{@code
 * POST /my-index/_sample/docs
 * {
 *   "docs": [
 *     { "message": "doc1", "value": 123 },
 *     { "message": "doc2", "value": 456 }
 *   ]
 * }
 * }</pre>
 */
@ServerlessScope(Scope.INTERNAL)
public class RestPostSampleDocsAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_sample/docs"));
    }

    @Override
    public String getName() {
        return "post_sample_docs_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        String[] indexNames = request.param("index").split(",");
        if (indexNames.length > 1) {
            throw new ActionRequestValidationException().addValidationError(
                "Can only add documents to a sample for a single index at a time, but found "
                    + Arrays.stream(indexNames).collect(Collectors.joining(", ", "[", "]"))
            );
        }

        String indexName = indexNames[0];

        // Parse the request body as an array of documents
        List<Map<String, Object>> documents = parseDocuments(request.contentParser());

        PostSampleDocsAction.Request postRequest = new PostSampleDocsAction.Request(indexName, documents);

        return channel -> client.execute(
            PostSampleDocsAction.INSTANCE,
            postRequest,
            new RestToXContentListener<PostSampleDocsAction.Response>(channel)
        );
    }

    /**
     * Parse the request body as an object containing a "docs" array.
     */
    private List<Map<String, Object>> parseDocuments(XContentParser parser) throws IOException {
        List<Map<String, Object>> documents = new ArrayList<>();

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("Request body must be a JSON object with a 'docs' field, but found [" + token + "]");
        }

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("docs".equals(currentFieldName)) {
                if (token != XContentParser.Token.START_ARRAY) {
                    throw new IllegalArgumentException("The 'docs' field must be an array, but found [" + token + "]");
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token != XContentParser.Token.START_OBJECT) {
                        throw new IllegalArgumentException(
                            "Each element in the 'docs' array must be a document object, but found [" + token + "]"
                        );
                    }
                    documents.add(parser.map());
                }
            } else {
                parser.skipChildren();
            }
        }

        if (documents.isEmpty()) {
            throw new IllegalArgumentException("Request body must contain a 'docs' field with at least one document");
        }

        return documents;
    }
}
