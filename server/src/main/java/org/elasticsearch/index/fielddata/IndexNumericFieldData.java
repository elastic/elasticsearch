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

import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

public interface IndexNumericFieldData extends IndexFieldData<LeafNumericFieldData> {

    enum NumericType {
        BOOLEAN(false, CoreValuesSourceType.BOOLEAN),
        BYTE(false, CoreValuesSourceType.NUMERIC),
        SHORT(false, CoreValuesSourceType.NUMERIC),
        INT(false, CoreValuesSourceType.NUMERIC),
        LONG(false, CoreValuesSourceType.NUMERIC),
        DATE(false, CoreValuesSourceType.DATE),
        DATE_NANOSECONDS(false, CoreValuesSourceType.DATE),
        HALF_FLOAT(true, CoreValuesSourceType.NUMERIC),
        FLOAT(true, CoreValuesSourceType.NUMERIC),
        DOUBLE(true, CoreValuesSourceType.NUMERIC);

        private final boolean floatingPoint;
        private final ValuesSourceType valuesSourceType;

        NumericType(boolean floatingPoint, ValuesSourceType valuesSourceType) {
            this.floatingPoint = floatingPoint;
            this.valuesSourceType = valuesSourceType;
        }

        public final boolean isFloatingPoint() {
            return floatingPoint;
        }
        public final ValuesSourceType getValuesSourceType() {
            return valuesSourceType;
        }
    }

    NumericType getNumericType();
}
