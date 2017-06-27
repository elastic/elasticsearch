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

package org.elasticsearch.ingest.common;

final class GrokMatchGroup {
    private static final String DEFAULT_TYPE = "string";
    private final String patternName;
    private final String fieldName;
    private final String type;
    private final String groupValue;

    GrokMatchGroup(String groupName, String groupValue) {
        String[] parts = groupName.split(":");
        patternName = parts[0];
        if (parts.length >= 2) {
            fieldName = parts[1];
        } else {
            fieldName = null;
        }

        if (parts.length == 3) {
            type = parts[2];
        } else {
            type = DEFAULT_TYPE;
        }
        this.groupValue = groupValue;
    }

    public String getName() {
        return (fieldName == null) ? patternName : fieldName;
    }

    public Object getValue() {
        if (groupValue == null) { return null; }

        switch(type) {
            case "int":
                return Integer.parseInt(groupValue);
            case "float":
                return Float.parseFloat(groupValue);
            default:
                return groupValue;
        }
    }
}
