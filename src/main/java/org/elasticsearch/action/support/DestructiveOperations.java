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

/**
 * Static helper for dealing with destructive operations and wildcards.
 */
public final class DestructiveOperations {

    public static final String REQUIRES_NAME = "action.destructive_requires_name";

    private DestructiveOperations() {
    }

    /**
     * Fail if there is wildcard usage in indices and the named is required for destructive operations.
     *
     * @param destructiveRequiresName Controls whether wildcard usage (*, prefix*, _all) is allowed
     */
    public static void failDestructive(String[] aliasesOrIndices, boolean destructiveRequiresName) {
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

    private static boolean hasWildcardUsage(String aliasOrIndex) {
        return "_all".equals(aliasOrIndex) || aliasOrIndex.indexOf('*') != -1;
    }

}
