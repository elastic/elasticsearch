/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.security.SecurityExtension;

/**
 * An extension point allowing to plug in custom functionality in x-pack authentication module.
 * @deprecated use {@link SecurityExtension} via SPI instead
 */
@Deprecated
public abstract class XPackExtension implements SecurityExtension {
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
     * Returns a list of settings that should be filtered from API calls. In most cases,
     * these settings are sensitive such as passwords.
     *
     * The value should be the full name of the setting or a wildcard that matches the
     * desired setting.
     * @deprecated use {@link Plugin#getSettingsFilter()} ()} via SPI extension instead
     */
    @Deprecated
    public List<String> getSettingsFilter() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return name();
    }
}
