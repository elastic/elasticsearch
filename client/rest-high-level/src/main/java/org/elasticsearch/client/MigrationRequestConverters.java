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
import org.elasticsearch.client.migration.DeprecationInfoRequest;
import org.elasticsearch.client.migration.GetFeatureUpgradeStatusRequest;
import org.elasticsearch.client.migration.PostFeatureUpgradeRequest;

final class MigrationRequestConverters {

    private MigrationRequestConverters() {}

    static Request getDeprecationInfo(DeprecationInfoRequest deprecationInfoRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addCommaSeparatedPathParts(deprecationInfoRequest.getIndices())
            .addPathPartAsIs("_migration", "deprecations")
            .build();

        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

    /**
     * Convert a {@link GetFeatureUpgradeStatusRequest} to a {@link Request}
     * @param getFeatureUpgradeStatusRequest a request for feature upgrade status
     * @return a {@link Request} with the correct path and HTTP request type
     */
    static Request getFeatureUpgradeStatus(GetFeatureUpgradeStatusRequest getFeatureUpgradeStatusRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_migration", "system_features").build();

        return new Request(HttpGet.METHOD_NAME, endpoint);
    }

    /**
     * Convert a {@link PostFeatureUpgradeRequest} to a {@link Request}
     * @param postFeatureUpgradeRequest a request for feature upgrade status
     * @return a {@link Request} with the correct path and HTTP request type
     */
    static Request postFeatureUpgrade(PostFeatureUpgradeRequest postFeatureUpgradeRequest) {
        String endpoint = new RequestConverters.EndpointBuilder().addPathPartAsIs("_migration", "system_features").build();

        return new Request(HttpPost.METHOD_NAME, endpoint);
    }
}
