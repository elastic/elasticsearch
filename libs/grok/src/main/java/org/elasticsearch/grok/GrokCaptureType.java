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

/**
 * The type defined for the field in the pattern.
 */
public enum GrokCaptureType {
    STRING {
        @Override
        protected Object parseValue(String str) {
            return str;
        }
    },
    INTEGER {
        @Override
        protected Object parseValue(String str) {
            return Integer.parseInt(str);
        }
    },
    LONG {
        @Override
        protected Object parseValue(String str) {
            return Long.parseLong(str);
        }
    },
    DOUBLE {
        @Override
        protected Object parseValue(String str) {
            return Double.parseDouble(str);
        }
    },
    FLOAT {
        @Override
        protected Object parseValue(String str) {
            return Float.parseFloat(str);
        }
    },
    BOOLEAN {
        @Override
        protected Object parseValue(String str) {
            return Boolean.parseBoolean(str);
        }
    };

    final Object parse(String str) {
        if (str == null) {
            return null;
        }
        return parseValue(str);
    }

    protected abstract Object parseValue(String str);

    static GrokCaptureType fromString(String str) {
        switch (str) {
            case "string":
                return STRING;
            case "int":
                return INTEGER;
            case "long":
                return LONG;
            case "double":
                return DOUBLE;
            case "float":
                return FLOAT;
            case "boolean":
                return BOOLEAN;
            default:
                return STRING;
        }
    }
}
