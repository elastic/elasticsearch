/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.test;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.Settings;

import java.util.Map;

abstract class NodeSettingsSource {

    public static final NodeSettingsSource EMPTY = new NodeSettingsSource() {
        @Override
        public Settings settings(int nodeOrdinal) {
            return null;
        }
    };

    /**
     * @return  the settings for the node represented by the given ordinal, or {@code null} if there are not settings defined (in which
     *          case a random settings will be generated for the node)
     */
    public abstract Settings settings(int nodeOrdinal);

    public static class Immutable extends NodeSettingsSource {

        private final Map<Integer, Settings> settingsPerNode;

        private Immutable(Map<Integer, Settings> settingsPerNode) {
            this.settingsPerNode = settingsPerNode;
        }

        public static Builder builder() {
            return new Builder();
        }

        @Override
        public Settings settings(int nodeOrdinal) {
            return settingsPerNode.get(nodeOrdinal);
        }

        public static class Builder {

            private final ImmutableMap.Builder<Integer, Settings> settingsPerNode = ImmutableMap.builder();

            private Builder() {
            }

            public Builder set(int ordinal, Settings settings) {
                settingsPerNode.put(ordinal, settings);
                return this;
            }

            public Immutable build() {
                return new Immutable(settingsPerNode.build());
            }

        }
    }
}
