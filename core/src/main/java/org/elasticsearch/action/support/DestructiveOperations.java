/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 * Helper for dealing with destructive operations and wildcard usage.
 */
public final class DestructiveOperations extends AbstractComponent implements NodeSettingsService.Listener {

    /**
     * Setting which controls whether wildcard usage (*, prefix*, _all) is allowed.
     */
    public static final String REQUIRES_NAME = "action.destructive_requires_name";
    private volatile boolean destructiveRequiresName;

    @Inject
    public DestructiveOperations(Settings settings, NodeSettingsService nodeSettingsService) {
        super(settings);
        destructiveRequiresName = settings.getAsBoolean(DestructiveOperations.REQUIRES_NAME, false);
        nodeSettingsService.addListener(this);
    }

    /**
     * Fail if there is wildcard usage in indices and the named is required for destructive operations.
     */
    public void failDestructive(String[] aliasesOrIndices) {
        if (!destructiveRequiresName) {
            return;
        }

        if (aliasesOrIndices == null || aliasesOrIndices.length == 0) {
            throw new IllegalArgumentException("Wildcard expressions or all indices are not allowed");
        } else if (aliasesOrIndices.length == 1) {
            if (hasWildcardUsage(aliasesOrIndices[0])) {
                throw new IllegalArgumentException("Wildcard expressions or all indices are not allowed");
            }
        } else {
            for (String aliasesOrIndex : aliasesOrIndices) {
                if (hasWildcardUsage(aliasesOrIndex)) {
                    throw new IllegalArgumentException("Wildcard expressions or all indices are not allowed");
                }
            }
        }
    }

    @Override
    public void onRefreshSettings(Settings settings) {
        boolean newValue = settings.getAsBoolean(DestructiveOperations.REQUIRES_NAME, destructiveRequiresName);
        if (destructiveRequiresName != newValue) {
            logger.info("updating [action.operate_all_indices] from [{}] to [{}]", destructiveRequiresName, newValue);
            this.destructiveRequiresName = newValue;
        }
    }

    private static boolean hasWildcardUsage(String aliasOrIndex) {
        return "_all".equals(aliasOrIndex) || aliasOrIndex.indexOf('*') != -1;
    }

}
