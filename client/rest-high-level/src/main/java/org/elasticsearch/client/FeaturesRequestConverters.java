/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.feature.GetFeaturesRequest;
import org.elasticsearch.client.feature.ResetFeaturesRequest;

public class FeaturesRequestConverters {

    private FeaturesRequestConverters() {}

    static Request getFeatures(GetFeaturesRequest getFeaturesRequest) {
        String endpoint = "/_features";
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params parameters = new RequestConverters.Params();
        parameters.withMasterTimeout(getFeaturesRequest.masterNodeTimeout());
        request.addParameters(parameters.asMap());
        return request;
    }

    static Request resetFeatures(ResetFeaturesRequest resetFeaturesRequest) {
        String endpoint = "/_features/_reset";
        return new Request(HttpPost.METHOD_NAME, endpoint);
    }
}
