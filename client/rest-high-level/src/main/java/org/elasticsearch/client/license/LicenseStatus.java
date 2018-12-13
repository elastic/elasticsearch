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
package org.elasticsearch.client.license;

/**
 * Status of an X-Pack license.
 */
public enum LicenseStatus {

    ACTIVE("active"),
    INVALID("invalid"),
    EXPIRED("expired");

    private final String label;

    LicenseStatus(String label) {
        this.label = label;
    }

    public String label() {
        return label;
    }

    public static LicenseStatus fromString(String value) {
        switch (value) {
            case "active":
                return ACTIVE;
            case "invalid":
                return INVALID;
            case "expired":
                return EXPIRED;
            default:
                throw new IllegalArgumentException("unknown license status [" + value + "]");
        }
    }
}
