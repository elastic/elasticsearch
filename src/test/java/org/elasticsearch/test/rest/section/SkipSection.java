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
package org.elasticsearch.test.rest.section;

import org.elasticsearch.test.rest.support.VersionUtils;

/**
 * Represents a skip section that tells whether a specific test section or suite needs to be skipped
 * based on the elasticsearch version the tests are running against.
 */
public class SkipSection {

    public static final SkipSection EMPTY = new SkipSection("", "");

    private final String version;
    private final String reason;

    public SkipSection(String version, String reason) {
        this.version = version;
        this.reason = reason;
    }

    public String getVersion() {
        return version;
    }

    public String getReason() {
        return reason;
    }

    public boolean skipVersion(String currentVersion) {
        if (isEmpty()) {
            return false;
        }
        return VersionUtils.skipCurrentVersion(version, currentVersion);
    }

    public boolean isEmpty() {
        return EMPTY.equals(this);
    }
}
