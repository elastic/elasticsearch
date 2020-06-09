/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.async;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.Collection;
import java.util.Collections;

public class AsyncPlugin extends Plugin implements SystemIndexPlugin {

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        return Collections.singletonList(new SystemIndexDescriptor(XPackPlugin.ASYNC_RESULTS_INDEX, this.getClass().getSimpleName()));
    }

}
