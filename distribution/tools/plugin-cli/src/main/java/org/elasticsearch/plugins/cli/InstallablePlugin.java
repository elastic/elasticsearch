/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.cli;

import org.elasticsearch.common.Strings;

import java.util.Objects;

/**
 * Models a single plugin that can be installed.
 */
public class InstallablePlugin {
    private String id;
    private String location;

    public InstallablePlugin() {}

    /**
     * Creates a new descriptor instance.
     *
     * @param id the name of the plugin. Cannot be null.
     * @param location the location from which to fetch the plugin, e.g. a URL or Maven
     *                 coordinates. Can be null for official plugins.
     */
    public InstallablePlugin(String id, String location) {
        this.id = Strings.requireNonBlank(id, "plugin id cannot be null or blank");
        this.location = location;
    }

    public InstallablePlugin(String id) {
        this(id, null);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = Strings.requireNonBlank(id, "plugin id cannot be null or blank");
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InstallablePlugin that = (InstallablePlugin) o;
        return id.equals(that.id) && Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, location);
    }

    @Override
    public String toString() {
        return "PluginDescriptor{id='" + id + "', location='" + location + "'}";
    }
}
