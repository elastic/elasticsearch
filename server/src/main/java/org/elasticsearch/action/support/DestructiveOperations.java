/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.List;

import java.util.Arrays;

/**
 * Helper for dealing with destructive operations and wildcard usage.
 */
public final class DestructiveOperations {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DestructiveOperations.class);

    /**
     * Setting which controls whether wildcard usage (*, prefix*, _all) is allowed.
     */
    public static final Setting<Boolean> REQUIRES_NAME_SETTING = Setting.boolSetting(
        "action.destructive_requires_name",
        false,
        Property.Dynamic,
        Property.NodeScope
    );

    static final String DESTRUCTIVE_REQUIRES_NAME_DEPRECATION_WARNING = "setting [action.destructive_requires_name] will "
        + "default to true in 8.0, set explicitly to false to preserve current behavior";

    /**
     * The "match none" pattern, "*,-*", will never actually be destructive
     * because it operates on no indices. If plugins or other components add
     * their own index resolution layers, they can pass this pattern to the
     * core code in order to indicate that an operation won't run on any
     * indices, relying on the core code to handle this situation.
     */
    private static final String[] MATCH_NONE_PATTERN = { "*", "-*" };
    private volatile boolean destructiveRequiresName;

    private volatile boolean isDestructiveRequiresNameSet;

    public DestructiveOperations(Settings settings, ClusterSettings clusterSettings) {
        destructiveRequiresName = REQUIRES_NAME_SETTING.get(settings);
        isDestructiveRequiresNameSet = REQUIRES_NAME_SETTING.exists(settings);
        clusterSettings.addSettingsUpdateConsumer(this::setDestructiveRequiresName, List.of(REQUIRES_NAME_SETTING));
    }

    private void setDestructiveRequiresName(Settings settings) {
        this.isDestructiveRequiresNameSet = REQUIRES_NAME_SETTING.exists(settings);
        this.destructiveRequiresName = REQUIRES_NAME_SETTING.get(settings);
    }

    /**
     * Fail if there is wildcard usage in indices and the named is required for destructive operations.
     */
    public void failDestructive(String[] aliasesOrIndices) {
        if (this.isDestructiveRequiresNameSet && this.destructiveRequiresName == false) {
            return;
        }

        if (aliasesOrIndices == null || aliasesOrIndices.length == 0) {
            checkWildCardOK();
        } else if (aliasesOrIndices.length == 1) {
            if (hasWildcardUsage(aliasesOrIndices[0])) {
                checkWildCardOK();
            }
        } else if (Arrays.equals(aliasesOrIndices, MATCH_NONE_PATTERN) == false) {
            for (String aliasesOrIndex : aliasesOrIndices) {
                if (hasWildcardUsage(aliasesOrIndex)) {
                    checkWildCardOK();
                }
            }
        }
    }

    private void checkWildCardOK() {
        if (this.isDestructiveRequiresNameSet == false) {
            deprecationLogger.critical(
                DeprecationCategory.SETTINGS,
                "destructive_requires_name_default",
                DESTRUCTIVE_REQUIRES_NAME_DEPRECATION_WARNING
            );
        }
        if (this.destructiveRequiresName) {
            throw new IllegalArgumentException("Wildcard expressions or all indices are not allowed");
        }
    }

    private static boolean hasWildcardUsage(String aliasOrIndex) {
        return "_all".equals(aliasOrIndex) || aliasOrIndex.indexOf('*') != -1;
    }

}
