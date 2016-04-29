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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class IndexConstraint {

    private final String field;
    private final Property property;
    private final Comparison comparison;
    private final String value;
    private final String optionalFormat;

    IndexConstraint(StreamInput input) throws IOException {
        this.field = input.readString();
        this.property = Property.read(input.readByte());
        this.comparison = Comparison.read(input.readByte());
        this.value = input.readString();
        if (input.getVersion().onOrAfter(Version.V_2_0_1)) {
            this.optionalFormat = input.readOptionalString();
        } else {
            this.optionalFormat = null;
        }
    }

    public IndexConstraint(String field, Property property, Comparison comparison, String value) {
        this(field, property, comparison, value, null);
    }

    public IndexConstraint(String field, Property property,
                           Comparison comparison, String value, String optionalFormat) {
        this.field = Objects.requireNonNull(field);
        this.property = Objects.requireNonNull(property);
        this.comparison = Objects.requireNonNull(comparison);
        this.value = Objects.requireNonNull(value);
        this.optionalFormat = optionalFormat;
    }

    /**
     * @return On what field the constraint is going to be applied on
     */
    public String getField() {
        return field;
    }

    /**
     * @return How to compare the specified value against the field property (lt, lte, gt and gte)
     */
    public Comparison getComparison() {
        return comparison;
    }

    /**
     * @return On what property of a field the constraint is going to be applied on (min or max value)
     */
    public Property getProperty() {
        return property;
    }

    /**
     * @return The value to compare against
     */
    public String getValue() {
        return value;
    }

    /**
     * @return An optional format, that specifies how the value string is converted in the native value of the field.
     *         Not all field types support this and right now only date field supports this option.
     */
    public String getOptionalFormat() {
        return optionalFormat;
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
