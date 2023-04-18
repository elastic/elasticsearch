/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

public final class BootstrapSettings {

    private BootstrapSettings() {}

    // TODO: remove this hack when insecure defaults are removed from java
    public static final Setting<Boolean> SECURITY_FILTER_BAD_DEFAULTS_SETTING = Setting.boolSetting(
        "security.manager.filter_bad_defaults",
        true,
        Property.NodeScope
    );

    public static final Setting<Boolean> MEMORY_LOCK_SETTING = Setting.boolSetting("bootstrap.memory_lock", false, Property.NodeScope);

    public static final Setting<Boolean> CTRLHANDLER_SETTING = Setting.boolSetting("bootstrap.ctrlhandler", true, Property.NodeScope);

}
