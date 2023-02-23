/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.Build;
import org.elasticsearch.core.Booleans;

public class FeatureFlag {

    private final String name;
    private final boolean enabled;

    public FeatureFlag(String name) {
        this.name = name;
        assert name.indexOf('.') == -1 : "Feature flag names may not contain a '.' character";
        assert name.contains("feature_flag") == false : "Feature flag names may not contain the string 'feature_flag'";

        final String propertyName = "es." + name + "_feature_flag_registered";
        if (Build.CURRENT.isSnapshot()) {
            enabled = parseSystemProperty(propertyName, true);
            if (enabled == false) {
                throw new IllegalArgumentException(
                    "Feature flag " + name + " (via system property '" + propertyName + "') cannot be disabled in snapshot builds"
                );
            }
        } else {
            enabled = parseSystemProperty(propertyName, false);
        }
    }

    private static boolean parseSystemProperty(String propertyName, boolean defaultValue) {
        final String propertyValue = System.getProperty(propertyName);
        try {
            return Booleans.parseBoolean(propertyValue, defaultValue);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid value [" + propertyValue + "] for system property [" + propertyName + "]", e);
        }
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public String toString() {
        return "Feature-Flag(" + name + "=" + enabled + ")";
    }
}
