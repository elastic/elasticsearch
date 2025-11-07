/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public class FilteredRestRequest extends RestRequest {

    private final RestRequest restRequest;
    private final String[] excludeFields;
    private BytesReference filteredBytes;

    public FilteredRestRequest(RestRequest restRequest, Set<String> excludeFields) {
        super(restRequest);
        this.restRequest = restRequest;
        this.excludeFields = excludeFields.toArray(String[]::new);
        this.filteredBytes = null;
    }

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
            final Map<String, Object> transformedSource = transformBody(result.v2());
            try {
                XContentBuilder xContentBuilder = XContentBuilder.builder(result.v1().xContent()).map(transformedSource);
                filteredBytes = BytesReference.bytes(xContentBuilder);
            } catch (IOException e) {
                throw new ElasticsearchException("failed to parse request", e);
            }
        }
        return ReleasableBytesReference.wrap(filteredBytes);
    }

    protected Map<String, Object> transformBody(Map<String, Object> map) {
        return XContentMapValues.filter(map, null, excludeFields);
    }
}
