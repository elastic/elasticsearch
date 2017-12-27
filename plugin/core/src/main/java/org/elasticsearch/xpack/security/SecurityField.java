/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.xpack.XpackField;

public final class SecurityField {

    private SecurityField() {}

    public static String setting(String setting) {
        assert setting != null && setting.startsWith(".") == false;
        return settingPrefix() + setting;
    }

    public static String settingPrefix() {
        return XpackField.featureSettingPrefix(XpackField.SECURITY) + ".";
    }
}
