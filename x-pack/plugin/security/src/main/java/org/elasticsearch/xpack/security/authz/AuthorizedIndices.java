/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.xpack.core.security.authz.permission.Role;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Abstraction used to make sure that we lazily load authorized indices only when requested and only maximum once per request. Also
 * makes sure that authorized indices don't get updated throughout the same request for the same user.
 */
class AuthorizedIndices {
    private final String action;
    private final MetaData metaData;
    private final Role userRoles;
    private List<String> authorizedIndices;

    AuthorizedIndices(Role userRoles, String action, MetaData metaData) {
        this.userRoles = userRoles;
        this.action = action;
        this.metaData = metaData;
    }

    List<String> get() {
        if (authorizedIndices == null) {
            authorizedIndices = load();
        }
        return authorizedIndices;
    }

    private List<String> load() {
        Predicate<String> predicate = userRoles.indices().allowedIndicesMatcher(action);

        List<String> indicesAndAliases = new ArrayList<>();
        // TODO: can this be done smarter? I think there are usually more indices/aliases in the cluster then indices defined a roles?
        for (Map.Entry<String, AliasOrIndex> entry : metaData.getAliasAndIndexLookup().entrySet()) {
            String aliasOrIndex = entry.getKey();
            if (predicate.test(aliasOrIndex)) {
                indicesAndAliases.add(aliasOrIndex);
            }
        }

        return Collections.unmodifiableList(indicesAndAliases);
    }
}
