/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security;

import org.apache.lucene.util.SPIClassIterator;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * An SPI extension point allowing to plug in custom functionality in x-pack authentication module.
 */
public interface SecurityExtension {

    /**
     * Returns authentication realm implementations added by this extension.
     *
     * The key of the returned {@link Map} is the type name of the realm, and the value
     * is a {@link Realm.Factory} which will construct
     * that realm for use in authentication when that realm type is configured.
     *
     * @param resourceWatcherService Use to watch configuration files for changes
     */
    default Map<String, Realm.Factory> getRealms(ResourceWatcherService resourceWatcherService) {
        return Collections.emptyMap();
    }

    /**
     * Returns a handler for authentication failures, or null to use the default handler.
     *
     * Only one installed extension may have an authentication failure handler. If more than
     * one extension returns a non-null handler, an error is raised.
     */
    default AuthenticationFailureHandler getAuthenticationFailureHandler() {
        return null;
    }

    /**
     * Returns an ordered list of role providers that are used to resolve role names
     * to {@link RoleDescriptor} objects.  Each provider is invoked in order to
     * resolve any role names not resolved by the reserved or native roles stores.
     *
     * Each role provider is represented as a {@link BiConsumer} which takes a set
     * of roles to resolve as the first parameter to consume and an {@link ActionListener}
     * as the second parameter to consume.  The implementation of the role provider
     * should be asynchronous if the computation is lengthy or any disk and/or network
     * I/O is involved.  The implementation is responsible for resolving whatever roles
     * it can into a set of {@link RoleDescriptor} instances.  If successful, the
     * implementation must wrap the set of {@link RoleDescriptor} instances in a
     * {@link RoleRetrievalResult} using {@link RoleRetrievalResult#success(Set)} and then invoke
     * {@link ActionListener#onResponse(Object)}.  If a failure was encountered, the
     * implementation should wrap the failure in a {@link RoleRetrievalResult} using
     * {@link RoleRetrievalResult#failure(Exception)} and then invoke
     * {@link ActionListener#onResponse(Object)} unless the failure needs to terminate the request,
     * in which case the implementation should invoke {@link ActionListener#onFailure(Exception)}.
     *
     * By default, an empty list is returned.
     *
     * @param settings The configured settings for the node
     * @param resourceWatcherService Use to watch configuration files for changes
     */
    default List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>>
        getRolesProviders(Settings settings, ResourceWatcherService resourceWatcherService) {
        return Collections.emptyList();
    }

    /**
     * Returns a authorization engine for authorizing requests, or null to use the default authorization mechanism.
     *
     * Only one installed extension may have an authorization engine. If more than
     * one extension returns a non-null authorization engine, an error is raised.
     *
     * @param settings The configured settings for the node
     */
    default AuthorizationEngine getAuthorizationEngine(Settings settings) {
        return null;
    }

    /**
     * Loads the XPackSecurityExtensions from the given class loader
     */
    static List<SecurityExtension> loadExtensions(ClassLoader loader) {
        SPIClassIterator<SecurityExtension> iterator = SPIClassIterator.get(SecurityExtension.class, loader);
        List<SecurityExtension> extensions = new ArrayList<>();
        while (iterator.hasNext()) {
            final Class<? extends SecurityExtension> c = iterator.next();
            try {
                extensions.add(c.getConstructor().newInstance());
            } catch (Exception e) {
                throw new ServiceConfigurationError("failed to load security extension [" + c.getName() + "]", e);
            }
        }
        return extensions;
    }

}
