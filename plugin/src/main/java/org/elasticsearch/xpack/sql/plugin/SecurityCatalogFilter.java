/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.xpack.sql.analysis.catalog.Catalog.GetIndexResult;
import org.elasticsearch.xpack.sql.analysis.catalog.EsIndex;
import org.elasticsearch.xpack.sql.analysis.catalog.FilteredCatalog;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Adapts SQL to x-pack security by intercepting calls to its catalog and
 * authorizing the user's view of every index, filtering out fields that
 * the user does not have access to.
 * <p>
 * Document level security is handled using the standard search mechanisms
 * but this class is required for SQL to respect field level security for
 * {@code SELECT} statements and index level security for metadata
 * statements like {@code SHOW TABLES} and {@code DESCRIBE TABLE}.
 */
public class SecurityCatalogFilter implements FilteredCatalog.Filter {
    private static final IndicesOptions OPTIONS = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;

    public SecurityCatalogFilter(ThreadContext threadContext, XPackLicenseState licenseState) {
        this.threadContext = threadContext;
        this.licenseState = licenseState;
    }

    @Override
    public GetIndexResult filterIndex(GetIndexResult delegateResult) {
        if (false == licenseState.isAuthAllowed()) {
            /* If security is disabled the index authorization won't be available.
             * It is technically possible there to be a race between licensing
             * being enabled and sql requests that might cause us to fail on those
             * requests but that should be rare. */
            return delegateResult;
        }
        EsIndex index = delegateResult.get();
        IndicesAccessControl control = getIndicesAccessControl();
        if (control == null) {
            // Looks like we're in a delayed response request so lets try that.
            BiFunction<IndicesOptions, String[], IndicesAccessControl> resolver = getAccessControlResolver();
            if (resolver == null) {
                // Looks like we're borked.
                throw new IllegalStateException("SQL request wasn't recognized properly by security");
            }
            control = resolver.apply(OPTIONS, new String[] {index.name()});
        }
        IndexAccessControl permissions = control.getIndexPermissions(index.name());
        /* Fetching the permissions always checks if they are granted. If they aren't
         * then it throws an exception. This is ok even for list requests because this
         * will only ever be called on indices that are authorized because of security's
         * request filtering. */
        if (false == permissions.getFieldPermissions().hasFieldLevelSecurity()) {
            return delegateResult;
        }
        Map<String, DataType> filteredMapping = new HashMap<>(index.mapping().size());
        for (Map.Entry<String, DataType> entry : index.mapping().entrySet()) {
            if (permissions.getFieldPermissions().grantsAccessTo(entry.getKey())) {
                filteredMapping.put(entry.getKey(), entry.getValue());
            }
        }
        return GetIndexResult.valid(new EsIndex(index.name(), filteredMapping, index.aliases(), index.settings()));
    }

    /**
     * Get the {@link IndicesAccessControl} for this request. This will return null for
     * requests that are not indices requests, like SQL's main action.
     */
    private IndicesAccessControl getIndicesAccessControl() {
        return threadContext.getTransient(AuthorizationService.INDICES_PERMISSIONS_KEY);
    }

    /**
     * Return a function that resolves permissions to the indices. This will return null
     * all actions other than "delayed" actions like the main SQL action.
     */
    private BiFunction<IndicesOptions, String[], IndicesAccessControl> getAccessControlResolver() {
        return threadContext.getTransient(AuthorizationService.INDICES_PERMISSIONS_RESOLVER_KEY);
    }
}
