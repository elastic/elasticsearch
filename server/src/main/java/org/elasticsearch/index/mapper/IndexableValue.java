/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper;

public interface IndexableValue {

    String stringValue();

    Number numberValue();

    boolean boolValue();

    byte[] binaryValue();

    <T> T objectValue(Class<T> type);

    IndexableValue MALFORMED = new ConcreteValue("malformed");

    static IndexableValue wrapBoolean(boolean value) {
        return new ConcreteValue("boolean") {
            @Override
            public boolean boolValue() {
                return value;
            }

            @Override
            public String stringValue() {
                return Boolean.toString(value);
            }
        };
    }

    static IndexableValue wrapBinary(byte[] bytes) {
        return new ConcreteValue("binary") {
            @Override
            public byte[] binaryValue() {
                return bytes;
            }
        };
    }

    static IndexableValue wrapNumber(Number number) {
        return new ConcreteValue("number") {
            @Override
            public Number numberValue() {
                return number;
            }
        };
    }

    static IndexableValue wrapObject(Object object) {
        return new ConcreteValue(object.getClass().getName()) {
            @Override
            public <T> T objectValue(Class<T> type) {
                if (object.getClass().isAssignableFrom(type)) {
                    return type.cast(object);
                }
                throw new UnsupportedOperationException("IndexableValue of type " + this.type + " does not support " + type + " values");
            }
        };
    }

    static IndexableValue wrapString(String value) {
        return new ConcreteValue("string") {
            @Override
            public String stringValue() {
                return value;
            }
        };
    }

    class ConcreteValue implements IndexableValue {

        protected final String type;

        public ConcreteValue(String type) {
            this.type = type;
        }

        @Override
        public String stringValue() {
            throw new UnsupportedOperationException("IndexableValue of type " + type + " does not support string values");
        }

        @Override
        public Number numberValue() {
            throw new UnsupportedOperationException("IndexableValue of type " + type + " does not support Number values");
        }

        @Override
        public boolean boolValue() {
            throw new UnsupportedOperationException("IndexableValue of type " + type + " does not support boolean values");
        }

        @Override
        public byte[] binaryValue() {
            throw new UnsupportedOperationException("IndexableValue of type " + type + " does not support binary values");
        }

        @Override
        public <T> T objectValue(Class<T> type) {
            throw new UnsupportedOperationException("IndexableValue of type " + type + " does not support object values");
        }
    }

}
