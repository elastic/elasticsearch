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

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.open.OpenIndexAction;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequest;

/**
 * Helper for dealing with destructive operations and wildcard usage.
 */
public final class DestructiveOperations extends AbstractComponent {

    /**
     * Setting which controls whether wildcard usage (*, prefix*, _all) is allowed.
     */
    public static final Setting<Boolean> REQUIRES_NAME_SETTING =
        Setting.boolSetting("action.destructive_requires_name", false, Property.Dynamic, Property.NodeScope);
    private volatile boolean destructiveRequiresName;

    public DestructiveOperations(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        destructiveRequiresName = REQUIRES_NAME_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(REQUIRES_NAME_SETTING,
                (destructiveRequiresName) -> this.destructiveRequiresName = destructiveRequiresName);
    }

    /**
     * Returns true if the action is destructive and must be blocked, based on its name and the indices it operates on.
     * An operation is considered destructive if it is disruptive and executed against all indices or wildcard expressions.
     * Whether destructive operations against all indices or wildcard expressions should be blocked or not can be controlled through the
     * "action.destructive_requires_name" cluster setting.
     */
    public boolean mustBlockDestructiveOperation(String action, TransportRequest request) {
        if (isDestructiveAction(action) && destructiveRequiresName) {
            assert request instanceof IndicesRequest;
            String[] aliasesOrIndices = ((IndicesRequest) request).indices();
            if (aliasesOrIndices == null || aliasesOrIndices.length == 0) {
                return true;
            }
            for (String aliasesOrIndex : aliasesOrIndices) {
                if (hasWildcardUsage(aliasesOrIndex)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isDestructiveAction(String action) {
        return DeleteIndexAction.NAME.equals(action) || CloseIndexAction.NAME.equals(action) || OpenIndexAction.NAME.equals(action);
    }

    private static boolean hasWildcardUsage(String aliasOrIndex) {
        return MetaData.ALL.equals(aliasOrIndex) || Regex.isSimpleMatchPattern(aliasOrIndex);
    }
}
