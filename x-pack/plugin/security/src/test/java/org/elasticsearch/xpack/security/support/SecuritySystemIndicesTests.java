/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.AbstractSystemIndexFormatVersionTests;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.security.Security;

import java.util.Collection;

public class SecuritySystemIndicesTests extends AbstractSystemIndexFormatVersionTests {

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors() {
        SystemIndexPlugin plugin = new Security(Settings.EMPTY);
        return plugin.getSystemIndexDescriptors(Settings.EMPTY);
    }
}
