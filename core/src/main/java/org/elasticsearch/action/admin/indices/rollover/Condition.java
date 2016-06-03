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

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;

public class Condition {

    public enum ConditionType {
        MAX_SIZE((byte) 0),
        MAX_AGE((byte) 1),
        MAX_DOCS((byte) 2);

        private final byte id;

        ConditionType(byte id) {
            this.id = id;
        }

        public byte getId() {
            return id;
        }

        public static ConditionType fromId(byte id) {
            if (id == 0) {
                return MAX_SIZE;
            } else if (id == 1) {
                return MAX_AGE;
            } else if (id == 2) {
                return MAX_DOCS;
            } else {
                throw new IllegalArgumentException("no condition type [" + id + "]");
            }
        }

        public static ConditionType fromString(String type) {
            final String typeString = type.toLowerCase(Locale.ROOT);
            switch (typeString) {
                case "max_size":
                    return MAX_SIZE;
                case "max_age":
                    return MAX_AGE;
                case "max_docs":
                    return MAX_DOCS;
                default:
                    throw new IllegalArgumentException("no condition type [" + type + "]");
            }
        }

        public static long parseFromString(ConditionType condition, String value) {
            switch (condition) {
                case MAX_SIZE:
                    return ByteSizeValue.parseBytesSizeValue(value, MAX_SIZE.name().toLowerCase(Locale.ROOT)).getBytes();
                case MAX_AGE:
                    return TimeValue.parseTimeValue(value, MAX_AGE.name().toLowerCase(Locale.ROOT)).getMillis();
                case MAX_DOCS:
                    try {
                       return Long.valueOf(value);
                    } catch (NumberFormatException e) {
                        throw new ElasticsearchParseException("Failed to parse setting [{}] with value [{}] as long", e,
                            MAX_DOCS.name().toLowerCase(Locale.ROOT), value);
                    }
                default:
                    throw new ElasticsearchParseException("condition [" + condition + "] not recognized");
            }
        }
    }

    private final ConditionType type;
    private final long value;

    public Condition(ConditionType type, long value) {
        this.type = type;
        this.value = value;
    }

    public ConditionType getType() {
        return type;
    }

    public long getValue() {
        return value;
    }
}
