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

package org.elasticsearch.index.fielddata;

/**
 */
public interface IndexNumericFieldData extends IndexFieldData<AtomicNumericFieldData> {

    public static enum NumericType {
        BOOLEAN(false),
        BYTE(false),
        SHORT(false),
        INT(false),
        LONG(false),
        HALF_FLOAT(true),
        FLOAT(true),
        DOUBLE(true);

        private final boolean floatingPoint;

        private NumericType(boolean floatingPoint) {
            this.floatingPoint = floatingPoint;
        }

        public final boolean isFloatingPoint() {
            return floatingPoint;
        }

    }

    NumericType getNumericType();
}
