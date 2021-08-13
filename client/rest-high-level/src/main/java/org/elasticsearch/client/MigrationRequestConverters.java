/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.migration.DeprecationInfoRequest;

final class MigrationRequestConverters {

    private MigrationRequestConverters() {
    }

    static Request getDeprecationInfo(DeprecationInfoRequest deprecationInfoRequest) {
        String endpoint = new RequestConverters.EndpointBuilder()
            .addCommaSeparatedPathParts(deprecationInfoRequest.getIndices())
            .addPathPartAsIs("_migration", "deprecations")
            .build();

        return new Request(HttpGet.METHOD_NAME, endpoint);
    }
}
