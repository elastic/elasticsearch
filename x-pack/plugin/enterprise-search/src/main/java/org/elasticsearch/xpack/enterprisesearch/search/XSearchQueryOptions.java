/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enterprisesearch.search;

import org.elasticsearch.rest.RestRequest;

import java.util.List;

public class XSearchQueryOptions {

    public final String queryString;

    public XSearchQueryOptions(String queryParam) {
        queryString = queryParam;
    }

    public XSearchQueryOptions(RestRequest request) {
        this(request.param("query"));
    }
}
