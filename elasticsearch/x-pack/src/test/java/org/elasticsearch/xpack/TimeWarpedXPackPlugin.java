/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.watcher.test.TimeWarpedWatcher;

public class TimeWarpedXPackPlugin extends XPackPlugin {

    public TimeWarpedXPackPlugin(Settings settings) {
        super(settings);
        watcher = new TimeWarpedWatcher(settings);
    }
}
