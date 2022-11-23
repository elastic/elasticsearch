/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.IndexQueryCache;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Opts out of the query cache if field level security is active for the current request, and it is unsafe to cache.
 */
public final class OptOutQueryCache extends IndexQueryCache {

    private static final Logger logger = LogManager.getLogger(IndexQueryCache.class);
    private final ThreadContext context;

    public OptOutQueryCache(final Index index, final IndicesQueryCache indicesQueryCache, final ThreadContext context) {
        super(index, indicesQueryCache);
        this.context = Objects.requireNonNull(context, "threadContext must not be null");
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        IndicesAccessControl indicesAccessControl = context.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        if (indicesAccessControl == null) {
            logger.debug("opting out of the query cache for index [{}]. current request doesn't hold indices permissions", index);
            return weight;
        }

        IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index.getName());
        if (indexAccessControl != null && indexAccessControl.getFieldPermissions().hasFieldLevelSecurity()) {
            if (cachingIsSafe(weight, indexAccessControl)) {
                logger.trace("not opting out of the query cache. request for index [{}] is safe to cache", index);
                return super.doCache(weight, policy);
            } else {
                logger.trace("opting out of the query cache. request for index [{}] is unsafe to cache", index);
                return weight;
            }
        } else {
            logger.trace("not opting out of the query cache. request for index [{}] has field level security disabled", index);
            return super.doCache(weight, policy);
        }
    }

    /**
     * Returns true if its safe to use the query cache for this query.
     */
    static boolean cachingIsSafe(Weight weight, IndicesAccessControl.IndexAccessControl permissions) {
        // support caching for common queries, by inspecting the field
        // TODO: If in the future there is a Query#extractFields() then we can do a better job
        Set<String> fields = new HashSet<>();
        try {
            FieldExtractor.extractFields(weight.getQuery(), fields);
        } catch (UnsupportedOperationException ok) {
            // we don't know how to safely extract the fields of this query, don't cache.
            return false;
        }

        // we successfully extracted the set of fields: check each one
        for (String field : fields) {
            // don't cache any internal fields (e.g. _field_names), these are complicated.
            if (field.startsWith("_") || permissions.getFieldPermissions().grantsAccessTo(field) == false) {
                return false;
            }
        }
        // we can cache, all fields are ok
        return true;
    }

}
