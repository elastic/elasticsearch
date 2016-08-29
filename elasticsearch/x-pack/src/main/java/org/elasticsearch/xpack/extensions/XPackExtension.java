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

import org.elasticsearch.xpack.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authc.Realm;


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
     */
    public Map<String, Realm.Factory> getRealms() {
        return Collections.emptyMap();
    }

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
