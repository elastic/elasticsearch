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

package org.elasticsearch.action.fieldstats;

import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Locale;

public class IndexConstraint {

    private final String field;
    private final Property property;
    private final Comparison comparison;
    private final String value;

    IndexConstraint(StreamInput input) throws IOException {
        this.field = input.readString();
        this.property = Property.read(input.readByte());
        this.comparison = Comparison.read(input.readByte());
        this.value = input.readString();
    }

    public IndexConstraint(String field, Property property, Comparison comparison, String value) {
        this.field = field;
        this.property = property;
        this.comparison = comparison;
        this.value = value;
    }

    public String getField() {
        return field;
    }

    public Comparison getComparison() {
        return comparison;
    }

    public Property getProperty() {
        return property;
    }

    public String getValue() {
        return value;
    }

    public enum Property {

        MIN((byte) 0),
        MAX((byte) 1);

        private final byte id;

        Property(byte id) {
            this.id = id;
        }

        public byte getId() {
            return id;
        }

        public static Property read(byte id) {
            switch (id) {
                case 0:
                    return MIN;
                case 1:
                    return MAX;
                default:
                    throw new IllegalArgumentException("Unknown property [" + id + "]");
            }
        }

        public static Property parse(String value) {
            value = value.toLowerCase(Locale.ROOT);
            switch (value) {
                case "min_value":
                    return MIN;
                case "max_value":
                    return MAX;
                default:
                    throw new IllegalArgumentException("Unknown property [" + value + "]");
            }
        }

    }

    public enum Comparison {

        LT((byte) 0),
        LTE((byte) 1),
        GT((byte) 2),
        GTE((byte) 3);

        private final byte id;

        Comparison(byte id) {
            this.id = id;
        }

        public byte getId() {
            return id;
        }

        public static Comparison read(byte id) {
            switch (id) {
                case 0:
                    return LT;
                case 1:
                    return LTE;
                case 2:
                    return GT;
                case 3:
                    return GTE;
                default:
                    throw new IllegalArgumentException("Unknown comparison [" + id + "]");
            }
        }

        public static Comparison parse(String value) {
            value = value.toLowerCase(Locale.ROOT);
            switch (value) {
                case "lt":
                    return LT;
                case "lte":
                    return LTE;
                case "gt":
                    return GT;
                case "gte":
                    return GTE;
                default:
                    throw new IllegalArgumentException("Unknown comparison [" + value + "]");
            }
        }

    }

}
