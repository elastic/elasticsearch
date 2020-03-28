/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.watcher;

import org.elasticsearch.common.settings.Setting;

import java.util.function.Function;

public class WatcherSettings {
    // This setting is only here for backward compatibility reasons as 6.x indices made use of it. It can be removed in 8.x.
    @Deprecated
    public static final Setting<String> INDEX_WATCHER_TEMPLATE_VERSION_SETTING =
            new Setting<>("index.xpack.watcher.template.version", "", Function.identity(), Setting.Property.IndexScope);
}
