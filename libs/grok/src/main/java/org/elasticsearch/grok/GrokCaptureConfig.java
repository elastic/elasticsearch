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

package org.elasticsearch.grok;

import org.joni.NameEntry;
import org.joni.Region;

import java.nio.charset.StandardCharsets;

/**
 * Configuration for a value that {@link Grok} can capture.
 */
public final class GrokCaptureConfig {
    private final String name;
    private final GrokCaptureType type;
    private final int[] backRefs;

    GrokCaptureConfig(NameEntry nameEntry) {
        String groupName = new String(nameEntry.name, nameEntry.nameP, nameEntry.nameEnd - nameEntry.nameP, StandardCharsets.UTF_8);
        String[] parts = groupName.split(":");
        name = parts.length >= 2 ? parts[1] : parts[0];
        type = parts.length == 3 ? GrokCaptureType.fromString(parts[2]) : GrokCaptureType.STRING;
        this.backRefs = nameEntry.getBackRefs();
    }

    /**
     * The name defined for the field in the pattern.
     */
    public String name() {
        return name;
    }

    /**
     * The type defined for the field in the pattern.
     */
    public GrokCaptureType type() {
        return type;
    }

    Object extract(byte[] textAsBytes, Region region) {
        for (int number : backRefs) {
            if (region.beg[number] >= 0) {
                String matchValue = new String(textAsBytes, region.beg[number], region.end[number] - region.beg[number],
                    StandardCharsets.UTF_8);
                return type.parse(matchValue);
            }
        }
        return null;
    }
}
