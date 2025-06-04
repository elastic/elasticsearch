/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Identifies an object that supplies a filter for the content of a {@link RestRequest}. This interface should be implemented by a
 * {@link org.elasticsearch.rest.RestHandler} that expects there will be sensitive content in the body of the request such as a password
 */
public interface RestRequestFilter {

    /**
     * Wraps the RestRequest and returns a version that provides the filtered content
     */
    default RestRequest getFilteredRequest(RestRequest restRequest) {
        Set<String> fields = getFilteredFields();
        if (restRequest.hasContent() && fields.isEmpty() == false) {
            return new RestRequest(restRequest) {

                private BytesReference filteredBytes = null;

                @Override
                public boolean hasContent() {
                    return true;
                }

                @Override
                public ReleasableBytesReference content() {
                    if (filteredBytes == null) {
                        Tuple<XContentType, Map<String, Object>> result = XContentHelper.convertToMap(
                            restRequest.requiredContent(),
                            true,
                            restRequest.getXContentType()
                        );
                        Map<String, Object> transformedSource = XContentMapValues.filter(
                            result.v2(),
                            null,
                            fields.toArray(Strings.EMPTY_ARRAY)
                        );
                        try {
                            XContentBuilder xContentBuilder = XContentBuilder.builder(result.v1().xContent()).map(transformedSource);
                            filteredBytes = BytesReference.bytes(xContentBuilder);
                        } catch (IOException e) {
                            throw new ElasticsearchException("failed to parse request", e);
                        }
                    }
                    return ReleasableBytesReference.wrap(filteredBytes);
                }
            };
        } else {
            return restRequest;
        }
    }

    /**
     * The list of fields that should be filtered. This can be a dot separated pattern to match sub objects and also supports wildcards
     */
    Set<String> getFilteredFields();
}
