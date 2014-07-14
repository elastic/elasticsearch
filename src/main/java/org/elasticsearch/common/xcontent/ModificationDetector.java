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

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Detects whether fields were modified within given tolerance rules.
 */
public class ModificationDetector implements Streamable {
    private Map<String, Field> fields;

    /**
     * Detect if a field was modified  within given tolerance rules. All arguments are non-nullable.
     * 
     * @param fieldPath
     *            path to the field
     * @param oldObj
     *            old value
     * @param newObj
     *            new value
     * @return is oldObj distinct from newObj using the rules under which this
     *         was configured
     */
    public boolean modified(String fieldPath, Object oldObj, Object newObj) {
        Field field = fields == null ? null : fields.get(fieldPath);
        if (field == null) {
            // Default to equals symantics
            return !oldObj.equals(newObj);
        }
        return field.modified(oldObj, newObj);
    }

    /**
     * Are there rules for individual fields?
     */
    public boolean hasFields() {
        return fields != null;
    }

    /**
     * Configure the tolerance rule for a field.
     * @param fieldPath path to the field
     * @param config rule config
     */
    public ModificationDetector fieldRule(String fieldPath, String config) {
        if (fields == null) {
            fields = new HashMap<>();
        }
        if (config.endsWith("%")) {
            try {
                fields.put(fieldPath, new PercentModified(Double.parseDouble(config.substring(0, config.length() - 1)) / 100));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(String.format(
                        "Field tolerance looks like a percent but didn't parse properly [%s].", config), e);
            }
            return this;
        }
        try {
            // If we don't what it is then try for an absolute number.
            fields.put(fieldPath, new AbsoluteNumericModified(Double.parseDouble(config)));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(String.format("Unknown field tolerance config [%s].", config));
        }
        return this;
    }

    private interface Field {
        /**
         * Was a field modified?
         */
        boolean modified(Object oldObj, Object newObj);
        /**
         * What is the rule required to build this?
         */
        String rule();
    }

    private abstract class NumberField implements Field {
        public boolean modified(Object oldObj, Object newObj) {
            Number oldNumber, newNumber;
            try {
                oldNumber = (Number) oldObj;
                newNumber = (Number) newObj;
            } catch (ClassCastException e) {
                throw new IllegalArgumentException(String.format(
                        "This modification detector is only appropriate for number fields but it received [%s] and [%s]", oldObj, newObj));
            }
            return numericModified(oldNumber, newNumber);
        }

        protected abstract boolean numericModified(Number oldNumber, Number newNumber);
    }

    /**
     * A number is different if it has changed by more than a percentage.
     */
    private class PercentModified extends NumberField {
        private final double tolerance;

        public PercentModified(double tolerance) {
            this.tolerance = tolerance;
        }

        @Override
        protected boolean numericModified(Number oldNumber, Number newNumber) {
            return Math.abs((oldNumber.doubleValue() - newNumber.doubleValue()) / oldNumber.doubleValue()) > tolerance;
        }

        @Override
        public String rule() {
            return (tolerance * 100.0) + "%";
        }
    }

    /**
     * A number is different if it has changed by more than some number.
     */
    private class AbsoluteNumericModified extends NumberField {
        private final double tolerance;

        public AbsoluteNumericModified(double tolerance) {
            this.tolerance = tolerance;
        }

        @Override
        protected boolean numericModified(Number oldNumber, Number newNumber) {
            return Math.abs(oldNumber.doubleValue() - newNumber.doubleValue()) > tolerance;
        }

        @Override
        public String rule() {
            return Double.toString(tolerance);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int rules = in.readInt();
        for (int r = 0; r < rules; r++) {
            fieldRule(in.readString(), in.readString());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (fields == null) {
            out.writeInt(0);
            return;
        }
        out.writeInt(fields.size());
        for (Map.Entry<String, Field> entry: fields.entrySet()) {
            out.writeString(entry.getKey());
            out.writeString(entry.getValue().rule());
        }
    }
}
