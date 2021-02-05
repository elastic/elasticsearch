/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.fleet;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;

import java.util.Collection;
import java.util.List;

/**
 * A plugin to manage and provide access to the system indices used by Fleet.
 *
 * Currently only exposes general-purpose APIs on {@code _fleet}-prefixed routes, to be more specialized as Fleet's requirements stabilize.
 */
public class Fleet extends Plugin implements SystemIndexPlugin {

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return List.of(
            new SystemIndexDescriptor(".fleet-servers*", "Configuration of fleet servers"),
            new SystemIndexDescriptor(".fleet-policies*", "Policies and enrollment keys"),
            new SystemIndexDescriptor(".fleet-agents*", "Agents and agent checkins"),
            new SystemIndexDescriptor(".fleet-actions*", "Fleet actions")
        );
    }
}
