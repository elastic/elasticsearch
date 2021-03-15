/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.snapshots.GetFeaturesRequest;

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
}
