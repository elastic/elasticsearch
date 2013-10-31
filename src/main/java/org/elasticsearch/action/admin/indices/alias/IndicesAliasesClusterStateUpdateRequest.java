/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.action.admin.indices.alias;

import org.elasticsearch.action.support.master.ClusterStateUpdateRequest;
import org.elasticsearch.cluster.metadata.AliasAction;

import java.util.Arrays;
import java.util.Locale;

/**
 * Cluster state update request that allows to add or remove aliases
 */
public class IndicesAliasesClusterStateUpdateRequest extends ClusterStateUpdateRequest<IndicesAliasesClusterStateUpdateRequest> {

    AliasAction[] actions;

    IndicesAliasesClusterStateUpdateRequest() {

    }

    /**
     * Returns the alias actions to be performed
     */
    public AliasAction[] actions() {
        return actions;
    }

    /**
     * Sets the alias actions to be executed
     */
    public IndicesAliasesClusterStateUpdateRequest actions(AliasAction[] actions) {
        this.actions = actions;
        return this;
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "indices aliases: %s", Arrays.toString(toString(actions)));
    }

    private String[] toString(AliasAction[] aliasActions) {
        String[] aliases = new String[aliasActions.length];
        for (int i = 0; i < aliasActions.length; i++) {
            AliasAction aliasAction = aliasActions[i];
            aliases[i] = String.format(Locale.ROOT, "%s alias [%s], index [%s]",
                    aliasAction.actionType().name(), aliasAction.alias(), aliasAction.index());
        }
        return aliases;
    }
}
