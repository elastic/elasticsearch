/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authc.Realm;
import org.elasticsearch.xpack.security.authc.RealmConfig;


/**
 * An extension point allowing to plug in custom functionality in x-pack authentication module.
 */
public abstract class XPackExtension {
    /**
     * The name of the plugin.
     */
    public abstract String name();

    /**
     * The description of the plugin.
     */
    public abstract String description();

    /**
     * Returns headers which should be copied from REST requests to internal cluster requests.
     */
    public Collection<String> getRestHeaders() {
        return Collections.emptyList();
    }

    /**
     * Returns authentication realm implementations added by this extension.
     *
     * The key of the returned {@link Map} is the type name of the realm, and the value
     * is a {@link org.elasticsearch.xpack.security.authc.Realm.Factory} which will construct
     * that realm for use in authentication when that realm type is configured.
     *
     * @param resourceWatcherService Use to watch configuration files for changes
     */
    public Map<String, Realm.Factory> getRealms(ResourceWatcherService resourceWatcherService) {
        return Collections.emptyMap();
    }

    /**
     * Returns the set of {@link Setting settings} that may be configured for the each type of realm.
     *
     * Each <em>setting key</em> must be unqualified and is in the same format as will be provided via {@link RealmConfig#settings()}.
     * If a given realm-type is not present in the returned map, then it will be treated as if it supported <em>all</em> possible settings.
     *
     * The life-cycle of an extension dictates that this method will be called before {@link #getRealms(ResourceWatcherService)}
     */
    public Map<String, Set<Setting<?>>> getRealmSettings() { return Collections.emptyMap(); }

    /**
     * Returns a handler for authentication failures, or null to use the default handler.
     *
     * Only one installed extension may have an authentication failure handler. If more than
     * one extension returns a non-null handler, an error is raised.
     */
    public AuthenticationFailureHandler getAuthenticationFailureHandler() {
        return null;
    }

    /**
     * Returns a list of settings that should be filtered from API calls. In most cases,
     * these settings are sensitive such as passwords.
     *
     * The value should be the full name of the setting or a wildcard that matches the
     * desired setting.
     */
    public List<String> getSettingsFilter() {
        return Collections.emptyList();
    }
}
