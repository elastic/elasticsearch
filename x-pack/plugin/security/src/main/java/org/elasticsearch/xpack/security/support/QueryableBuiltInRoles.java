/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.Collection;
import java.util.Map;

/**
 * A class that holds the built-in roles and their hash digests.
 */
public record QueryableBuiltInRoles(Map<String, String> rolesDigest, Collection<RoleDescriptor> roleDescriptors) {

    /**
     * A listener that is notified when the built-in roles change.
     */
    public interface Listener {

        /**
         * Called when the built-in roles change.
         *
         * @param roles the new built-in roles.
         */
        void onRolesChanged(QueryableBuiltInRoles roles);

    }

    /**
     * A provider that provides the built-in roles and can notify subscribed listeners when the built-in roles change.
     */
    public interface Provider {

        /**
         * @return the built-in roles.
         */
        QueryableBuiltInRoles getRoles();

        /**
         * Adds a listener to be notified when the built-in roles change.
         *
         * @param listener the listener to add.
         */
        void addListener(Listener listener);

    }
}
