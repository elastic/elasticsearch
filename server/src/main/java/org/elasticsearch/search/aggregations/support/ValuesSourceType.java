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

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.DocValueFormat;

import java.util.function.LongSupplier;

/**
 * ValuesSourceType wraps the creation of specific per-source instances each {@link ValuesSource} needs to provide.  Every top-level
 * subclass of {@link ValuesSource} should have a corresponding implementation of this interface.  In general, new data types seeking
 * aggregation support should create a top level {@link ValuesSource}, then implement this to return wrappers for the specific sources of
 * values.
 */
public interface ValuesSourceType {
    /**
     * Called when an aggregation is operating over a known empty set (usually because the field isn't specified), this method allows for
     * returning a no-op implementation.  All {@link ValuesSource}s should implement this method.
     * @return - Empty specialization of the base {@link ValuesSource}
     */
    ValuesSource getEmpty();

    /**
     * Returns the type-specific sub class for a script data source.  {@link ValuesSource}s that do not support scripts should throw
     * {@link org.elasticsearch.search.aggregations.AggregationExecutionException}.  Note that this method is called when a script is
     * operating without an underlying field.  Scripts operating over fields are handled by the script argument to getField below.
     *
     * @param script - The script being wrapped
     * @param scriptValueType - The expected output type of the script
     * @return - Script specialization of the base {@link ValuesSource}
     */
    ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType);

    /**
     * Return a {@link ValuesSource} wrapping a field for the given type.  All {@link ValuesSource}s must implement this method.
     *
     * @param fieldContext - The field being wrapped
     * @param script - Optional script that might be applied over the field
     * @return - Field specialization of the base {@link ValuesSource}
     */
    ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script);

    /**
     * Apply the given missing value to an already-constructed {@link ValuesSource}.  Types which do not support missing values should throw
     * {@link org.elasticsearch.search.aggregations.AggregationExecutionException}
     *
     * @param valuesSource - The original {@link ValuesSource}
     * @param rawMissing - The missing value we got from the parser, typically a string or number
     * @param docValueFormat - The format to use for further parsing the user supplied value, e.g. a date format
     * @param now - Used in conjunction with the formatter, should return the current time in milliseconds
     * @return - Wrapper over the provided {@link ValuesSource} to apply the given missing value
     */
    ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat,
                                LongSupplier now);
}
