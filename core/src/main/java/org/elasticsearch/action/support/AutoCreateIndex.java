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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;

/**
 * Encapsulates the logic of whether a new index should be automatically created when
 * a write operation is about to happen in a non existing index.
 */
public final class AutoCreateIndex {

    private final boolean needToCheck;
    private final boolean globallyDisabled;
    private final String[] matches;
    private final String[] matches2;
    private final IndexNameExpressionResolver resolver;

    @Inject
    public AutoCreateIndex(Settings settings, IndexNameExpressionResolver resolver) {
        this.resolver = resolver;
        String value = settings.get("action.auto_create_index");
        if (value == null || Booleans.isExplicitTrue(value)) {
            needToCheck = true;
            globallyDisabled = false;
            matches = null;
            matches2 = null;
        } else if (Booleans.isExplicitFalse(value)) {
            needToCheck = false;
            globallyDisabled = true;
            matches = null;
            matches2 = null;
        } else {
            needToCheck = true;
            globallyDisabled = false;
            matches = Strings.commaDelimitedListToStringArray(value);
            matches2 = new String[matches.length];
            for (int i = 0; i < matches.length; i++) {
                matches2[i] = matches[i].substring(1);
            }
        }
    }

    /**
     * Do we really need to check if an index should be auto created?
     */
    public boolean needToCheck() {
        return this.needToCheck;
    }

    /**
     * Should the index be auto created?
     */
    public boolean shouldAutoCreate(String index, ClusterState state) {
        if (!needToCheck) {
            return false;
        }
        boolean exists = resolver.hasIndexOrAlias(index, state);
        if (exists) {
            return false;
        }
        if (globallyDisabled) {
            return false;
        }
        // matches not set, default value of "true"
        if (matches == null) {
            return true;
        }
        for (int i = 0; i < matches.length; i++) {
            char c = matches[i].charAt(0);
            if (c == '-') {
                if (Regex.simpleMatch(matches2[i], index)) {
                    return false;
                }
            } else if (c == '+') {
                if (Regex.simpleMatch(matches2[i], index)) {
                    return true;
                }
            } else {
                if (Regex.simpleMatch(matches[i], index)) {
                    return true;
                }
            }
        }
        return false;
    }
}
