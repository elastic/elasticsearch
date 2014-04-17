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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.settings.NodeSettingsService;

/**
 * Helper for dealing with destructive operations and wildcard usage.
 */
public final class DestructiveOperations {

    /**
     * Setting which controls whether wildcard usage (*, prefix*, _all) is allowed.
     */
    public static final String REQUIRES_NAME = "action.destructive_requires_name";

    public static final boolean DEFAULT_REQUIRES_NAME = false;

    private final ESLogger logger;
    private volatile boolean destructiveRequiresName = DEFAULT_REQUIRES_NAME;

    // TODO: Turn into a component that can be reused and wired up into all the transport actions where
    // this helper logic is required. Note: also added the logger as argument, otherwise the same log
    // statement is printed several times, this can removed once this becomes a component.
    public DestructiveOperations(ESLogger logger, Settings settings, NodeSettingsService nodeSettingsService) {
        this.logger = logger;
        destructiveRequiresName = settings.getAsBoolean(DestructiveOperations.REQUIRES_NAME, false);
        nodeSettingsService.addListener(new ApplySettings(settings));
    }

    /**
     * Fail if there is wildcard usage in indices and the named is required for destructive operations.
     */
    public void failDestructive(String[] aliasesOrIndices) {
        if (!destructiveRequiresName) {
            return;
        }

        if (aliasesOrIndices == null || aliasesOrIndices.length == 0) {
            throw new ElasticsearchIllegalArgumentException("Wildcard expressions or all indices are not allowed");
        } else if (aliasesOrIndices.length == 1) {
            if (hasWildcardUsage(aliasesOrIndices[0])) {
                throw new ElasticsearchIllegalArgumentException("Wildcard expressions or all indices are not allowed");
            }
        } else {
            for (String aliasesOrIndex : aliasesOrIndices) {
                if (hasWildcardUsage(aliasesOrIndex)) {
                    throw new ElasticsearchIllegalArgumentException("Wildcard expressions or all indices are not allowed");
                }
            }
        }
    }

    class ApplySettings extends NodeSettingsService.Listener {

        public ApplySettings(Settings settings) {
            super(settings);
        }

        @Override
        public void onRefreshSettings(Settings settings) {
            boolean newValue = settings.getAsBoolean(REQUIRES_NAME, DEFAULT_REQUIRES_NAME);
            if (destructiveRequiresName != newValue) {
                logger.info("updating [action.operate_all_indices] from [{}] to [{}]", destructiveRequiresName, newValue);
                DestructiveOperations.this.destructiveRequiresName = newValue;
            }
        }
    }

    private static boolean hasWildcardUsage(String aliasOrIndex) {
        return "_all".equals(aliasOrIndex) || aliasOrIndex.indexOf('*') != -1;
    }

}
