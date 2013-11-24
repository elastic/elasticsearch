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

package org.elasticsearch.search.aggregations.support.numeric;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.aggregations.support.FieldDataSource;
import org.elasticsearch.search.aggregations.support.ValuesSource;

/**
 * A source of numeric data.
 */
public final class NumericValuesSource implements ValuesSource {

    private final FieldDataSource.Numeric source;
    private final ValueFormatter formatter;
    private final ValueParser parser;

    public NumericValuesSource(FieldDataSource.Numeric source, @Nullable ValueFormatter formatter, @Nullable ValueParser parser) {
        this.source = source;
        this.formatter = formatter;
        this.parser = parser;
    }

    @Override
    public BytesValues bytesValues() {
        return source.bytesValues();
    }

    public boolean isFloatingPoint() {
        return source.isFloatingPoint();
    }

    public LongValues longValues() {
        return source.longValues();
    }

    public DoubleValues doubleValues() {
        return source.doubleValues();
    }

    public ValueFormatter formatter() {
        return formatter;
    }

    public ValueParser parser() {
        return parser;
    }

}
