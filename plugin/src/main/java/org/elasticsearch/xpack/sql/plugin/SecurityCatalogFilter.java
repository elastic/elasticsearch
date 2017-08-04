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
    // NOCOMMIT need to figure out sql on aliases that expand to many indices
    private static final IndicesOptions OPTIONS = IndicesOptions.strictSingleIndexNoExpandForbidClosed();

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;

    public SecurityCatalogFilter(ThreadContext threadContext, XPackLicenseState licenseState) {
        this.threadContext = threadContext;
        this.licenseState = licenseState;
    }

    @Override
    public EsIndex filterIndex(EsIndex index) {
        if (false == licenseState.isAuthAllowed()) {
            /* If security is disabled the index authorization won't be available.
             * It is technically possible there to be a race between licensing
             * being enabled and sql requests that might cause us to fail on those
             * requests but that should be rare. */
            return index;
        }
        IndexAccessControl permissions = getAccessControlResolver()
                .apply(OPTIONS, new String[] {index.name()})
                .getIndexPermissions(index.name());
        /* Fetching the permissions always checks if they are granted. If they aren't
         * then it throws an exception. */
        if (false == permissions.getFieldPermissions().hasFieldLevelSecurity()) {
            return index;
        }
        Map<String, DataType> filteredMapping = new HashMap<>(index.mapping().size());
        for (Map.Entry<String, DataType> entry : index.mapping().entrySet()) {
            if (permissions.getFieldPermissions().grantsAccessTo(entry.getKey())) {
                filteredMapping.put(entry.getKey(), entry.getValue());
            }
        }
        return new EsIndex(index.name(), filteredMapping, index.aliases(), index.settings());
    }

    private BiFunction<IndicesOptions, String[], IndicesAccessControl> getAccessControlResolver() {
        BiFunction<IndicesOptions, String[], IndicesAccessControl> resolver =
                threadContext.getTransient(AuthorizationService.INDICES_PERMISSIONS_RESOLVER_KEY);
        if (resolver == null) {
            throw new IllegalStateException("SQL request wasn't recognized properly by security");
        }
        return resolver;
    }
}
