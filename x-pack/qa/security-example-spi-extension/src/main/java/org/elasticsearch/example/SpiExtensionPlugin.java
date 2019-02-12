/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.example;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.example.realm.CustomRealm;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * An example x-pack extension for testing custom realms and custom role providers.
 */
public class SpiExtensionPlugin extends Plugin implements ActionPlugin {

    @Override
    public Collection<String> getRestHeaders() {
        return Arrays.asList(CustomRealm.USER_HEADER, CustomRealm.PW_HEADER);
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> list = new ArrayList<>(RealmSettings.getStandardSettings(CustomRealm.TYPE));
        list.add(RealmSettings.simpleString(CustomRealm.TYPE, "filtered_setting", Setting.Property.NodeScope, Setting.Property.Filtered));
        return list;
    }
}
