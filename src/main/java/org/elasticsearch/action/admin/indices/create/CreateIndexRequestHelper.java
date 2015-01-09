/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.admin.indices.alias.Alias;

import java.util.Set;

/*
 * Helper needed to retrieve aliases from a CreateIndexRequest, as the corresponding getter has package private visibility
 * TODO Remove this class as soon as es core 1.5.0 is out
 */
public final class CreateIndexRequestHelper {

    private CreateIndexRequestHelper() {
    }

    public static Set<Alias> aliases(CreateIndexRequest createIndexRequest) {
        return createIndexRequest.aliases();
    }
}
