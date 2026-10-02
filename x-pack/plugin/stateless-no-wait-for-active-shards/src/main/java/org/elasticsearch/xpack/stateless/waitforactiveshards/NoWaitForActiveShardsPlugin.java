/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.waitforactiveshards;

import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

public class NoWaitForActiveShardsPlugin extends Plugin implements ActionPlugin {
    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(new SuppressWaitForActiveShardsActionFilter());
    }
}
