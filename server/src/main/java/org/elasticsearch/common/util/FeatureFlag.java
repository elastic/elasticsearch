/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Build;
import org.elasticsearch.core.Booleans;

import java.util.function.Function;

/**
 * A utility class for registering feature flags in Elasticsearch code.
 * <br/>
 * Typical usage:
 * <pre>{
 *    public static final FeatureFlag XYZZY_FEATURE_FLAG = new FeatureFlag("xyzzy");
 *    // ...
 *    if (XYZZY_FEATURE_FLAG.isEnabled()) {
 *        addRestHandlers();
 *        registerSettings();
 *        // etc
 *    }
 * }</pre>
 * <p>
 * The feature flag will be enabled automatically in all {@link Build#isSnapshot() snapshot} builds.
 * The feature flag can be enabled in release builds by setting the system property "es.{name}_feature_flag_enabled" to {@code true}
 * (e.g. {@code -Des.xyzzy_feature_flag_enabled=true}
 * </p>
 */
public class FeatureFlag {

    private final Logger logger = LogManager.getLogger(FeatureFlag.class);

    private final String name;
    private final boolean enabled;

    private static final Function<String, String> GET_SYSTEM_PROPERTY = System::getProperty;

    public FeatureFlag(String name) {
        this(name, "enabled", Build.CURRENT, GET_SYSTEM_PROPERTY);
    }

    /**
     * This method exists to register feature flags that use the old {@code {name}_feature_flag_registered} naming for their system
     * property, instead of the preferred {@code {name}_feature_flag_enabled} name.
     * It exists so that old style feature flag implementations can be converted to use this utility class without changing the name of
     * their system property (which would affect consumers of the feature).
     * @deprecated Use {@link FeatureFlag#FeatureFlag(String)} instead
     */
    @Deprecated
    public static FeatureFlag legacyRegisteredFlag(String name) {
        return new FeatureFlag(name, "registered", Build.CURRENT, GET_SYSTEM_PROPERTY);
    }

    /**
     * Accessible for testing only
     */
    FeatureFlag(String name, String suffix, Build build, Function<String, String> getSystemProperty) {
        this.name = name;
        assert name.indexOf('.') == -1 : "Feature flag names may not contain a '.' character";
        assert name.contains("feature_flag") == false : "Feature flag names may not contain the string 'feature_flag'";

        final String propertyName = "es." + name + "_feature_flag_" + suffix;
        if (build.isSnapshot()) {
            enabled = parseSystemProperty(getSystemProperty, propertyName, true);
            if (enabled == false) {
                throw new IllegalArgumentException(
                    "Feature flag " + name + " (via system property '" + propertyName + "') cannot be disabled in snapshot builds"
                );
            }
            logger.info("The current build is a snapshot, feature flag [{}] is enabled", name);
        } else {
            enabled = parseSystemProperty(getSystemProperty, propertyName, false);
            logger.debug("The current build is a not snapshot, feature flag [{}] is {}", name, enabled ? "enabled" : "disabled");
        }
    }

    private boolean parseSystemProperty(Function<String, String> getProperty, String propertyName, boolean defaultValue) {
        final String propertyValue = getProperty.apply(propertyName);
        logger.trace("Feature flag system property [{}] is set to [{}]", propertyName, propertyValue);
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
