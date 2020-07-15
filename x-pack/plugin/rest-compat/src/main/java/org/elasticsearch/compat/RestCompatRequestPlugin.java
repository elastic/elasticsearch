/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.compat;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.rest.RestHeaderDefinition;
import org.elasticsearch.xpack.core.security.SecuritySettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.UnaryOperator;

public class RestCompatRequestPlugin extends Plugin implements ActionPlugin {
    static final Setting<String> SIMPLE_SETTING = Setting.simpleString("compat.setting", Setting.Property.NodeScope);
    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(SIMPLE_SETTING);
    }

    @Override
    public Settings additionalSettings() {
        final Settings.Builder builder = Settings.builder();
        builder.put("compat.setting", true);
        return builder.build();
    }

}
