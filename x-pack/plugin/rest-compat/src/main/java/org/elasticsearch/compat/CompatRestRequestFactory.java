/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.compat;

import org.elasticsearch.plugins.spi.RestRequestFactoryProvider;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestRequestFactory2;

public class CompatRestRequestFactory implements RestRequestFactoryProvider {
    @Override
    public RestRequestFactory2 getRestRequestFactory() {
        return new RestRequestFactory2() {
            @Override
            public RestRequest enrich(RestRequest restRequest) {
                return new RestCompatRequestPlugin.CompatibleRestRequest(restRequest);
            }
        };
    }
}
