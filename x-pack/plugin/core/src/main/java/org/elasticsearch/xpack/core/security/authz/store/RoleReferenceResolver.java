/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference.ServiceAccountRoleReference;

/**
 * Implementation of this interface knows how to turn different subtypes of {@link RoleReference} into concrete role descriptors.
 */
public interface RoleReferenceResolver {

    void resolveNamedRoleReference(RoleReference.NamedRoleReference namedRoleReference, ActionListener<RolesRetrievalResult> listener);

    void resolveApiKeyRoleReference(RoleReference.ApiKeyRoleReference apiKeyRoleReference, ActionListener<RolesRetrievalResult> listener);

    void resolveBwcApiKeyRoleReference(
        RoleReference.BwcApiKeyRoleReference bwcApiKeyRoleReference,
        ActionListener<RolesRetrievalResult> listener
    );

    void resolveServiceAccountRoleReference(ServiceAccountRoleReference roleReference, ActionListener<RolesRetrievalResult> listener);
}
