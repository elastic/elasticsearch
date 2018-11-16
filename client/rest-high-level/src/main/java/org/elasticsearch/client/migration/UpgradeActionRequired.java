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
package org.elasticsearch.client.migration;

import java.util.Locale;

/**
 * Indicates the type of the upgrade required for the index
 */
public enum UpgradeActionRequired {
    NOT_APPLICABLE,   // Indicates that the check is not applicable to this index type, the next check will be performed
    UP_TO_DATE,       // Indicates that the check finds this index to be up to date - no additional checks are required
    REINDEX,          // The index should be reindex
    UPGRADE;          // The index should go through the upgrade procedure

    public static UpgradeActionRequired fromString(String value) {
        return UpgradeActionRequired.valueOf(value.toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

}
