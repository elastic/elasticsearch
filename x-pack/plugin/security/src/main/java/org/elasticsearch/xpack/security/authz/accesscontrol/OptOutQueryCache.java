/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.accesscontrol;

import org.apache.lucene.search.QueryCachingPolicy;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.query.QueryCache;
import org.elasticsearch.indices.IndicesQueryCache;
import org.elasticsearch.license.LicenseStateListener;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Opts out of the query cache if field level security is active for the current request, and it is unsafe to cache. Note that the method
 * {@link #listenForLicenseStateChanges()} must be invoked after construction of the query cache and before any other public methods are
 * invoked on this query cache.
 */
public final class OptOutQueryCache extends AbstractIndexComponent implements LicenseStateListener, QueryCache {

    private final IndicesQueryCache indicesQueryCache;
    private final ThreadContext context;
    private final String indexName;
    private final XPackLicenseState licenseState;
    private volatile boolean licenseStateListenerRegistered;

    public OptOutQueryCache(
            final IndexSettings indexSettings,
            final IndicesQueryCache indicesQueryCache,
            final ThreadContext context,
            final XPackLicenseState licenseState) {
        super(indexSettings);
        this.indicesQueryCache = indicesQueryCache;
        this.context = Objects.requireNonNull(context, "threadContext must not be null");
        this.indexName = indexSettings.getIndex().getName();
        this.licenseState = Objects.requireNonNull(licenseState, "licenseState");
    }

    /**
     * Register this query cache to listen for license state changes. This must be done after construction of this query cache before any
     * other public methods are invoked on this query cache.
     */
    public void listenForLicenseStateChanges() {
        /*
         * Registering this as a listener can not be done in the constructor because otherwise it would be unsafe publication of this. That
         * is, it would expose this to another thread before the constructor had finished. Therefore, we have a dedicated method to register
         * the listener that is invoked after the constructor has returned.
         */
        assert licenseStateListenerRegistered == false;
        licenseState.addListener(this);
        licenseStateListenerRegistered = true;
    }

    @Override
    public void close() throws ElasticsearchException {
        assert licenseStateListenerRegistered;
        licenseState.removeListener(this);
        clear("close");
    }

    @Override
    public void licenseStateChanged() {
        assert licenseStateListenerRegistered;
        clear("license state changed");
    }

    @Override
    public void clear(final String reason) {
        assert licenseStateListenerRegistered;
        logger.debug("full cache clear, reason [{}]", reason);
        indicesQueryCache.clearIndex(index().getName());
    }

    @Override
    public Weight doCache(Weight weight, QueryCachingPolicy policy) {
        assert licenseStateListenerRegistered;
        if (licenseState.isAuthAllowed() == false) {
            logger.debug("not opting out of the query cache; authorization is not allowed");
            return indicesQueryCache.doCache(weight, policy);
        }

        IndicesAccessControl indicesAccessControl = context.getTransient(
                AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
        if (indicesAccessControl == null) {
            logger.debug("opting out of the query cache. current request doesn't hold indices permissions");
            return weight;
        }

        IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(indexName);
        if (indexAccessControl != null && indexAccessControl.getFieldPermissions().hasFieldLevelSecurity()) {
            if (cachingIsSafe(weight, indexAccessControl)) {
                logger.trace("not opting out of the query cache. request for index [{}] is safe to cache", indexName);
                return indicesQueryCache.doCache(weight, policy);
            } else {
                logger.trace("opting out of the query cache. request for index [{}] is unsafe to cache", indexName);
                return weight;
            }
        } else {
            logger.trace("not opting out of the query cache. request for index [{}] has field level security disabled", indexName);
            return indicesQueryCache.doCache(weight, policy);
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
