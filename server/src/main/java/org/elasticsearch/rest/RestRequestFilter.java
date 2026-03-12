/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

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
            return new FilteredRestRequest(restRequest, fields);
        } else {
            return restRequest;
        }
    }

    /**
     * The list of fields that should be filtered. This can be a dot separated pattern to match sub objects and also supports wildcards
     */
    Set<String> getFilteredFields();

}
