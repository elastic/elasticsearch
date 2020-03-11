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

import java.time.ZoneId;
import java.util.function.LongSupplier;

/**
 * {@link ValuesSourceType}s are the quantum unit of aggregations support.  {@link org.elasticsearch.index.mapper.MappedFieldType}s that
 * allow aggregations map to exactly one ValuesSourceType, although multiple field types can map to the same ValuesSourceType.  Aggregations
 * in turn provide a set of ValuesSourceTypes they can operate on.  ValuesSourceTypes in turn map to a single direct sub-class of
 * {@link ValuesSource} (e.g. {@link ValuesSource.Numeric}; note that a given ValuesSourceType can yield different sub-sub-classes, e.g.
 * {@link ValuesSource.Numeric.WithScript}, depending on the configuration).  Note that it's possible that two different ValuesSourceTypes
 * will yield the same ValuesSource subclass.  This typically happens when the underlying representation is shared, but logically the data
 * are different, such as with numbers and dates.
 *
 * ValuesSourceTypes should be stateless, and thus immutable.  We recommend that plugins define an enum for their ValuesSourceTypes, even
 * if the plugin only intends to define one ValuesSourceType.  ValuesSourceTypes are not serialized as part of the aggregations framework.
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
     * @param nowSupplier - Used in conjunction with the formatter, should return the current time in milliseconds
     * @return - Wrapper over the provided {@link ValuesSource} to apply the given missing value
     */
    ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat,
                                LongSupplier nowSupplier);

    /**
     * This method provides a hook for specifying a type-specific formatter.  When {@link ValuesSourceConfig} can resolve a
     * {@link org.elasticsearch.index.mapper.MappedFieldType}, it prefers to get the formatter from there.  Only when a field can't be
     * resolved (which is to say script cases and unmapped field cases), it will fall back to calling this method on whatever
     * {@link ValuesSourceType} it was able to resolve to.
     *
     * @param format - User supplied format string (Optional)
     * @param tz - User supplied time zone (Optional)
     * @return - A formatter object, configured with the passed in settings if appropriate.
     */
    default DocValueFormat getFormatter(String format, ZoneId tz) {
        return DocValueFormat.RAW;
    }

    // TODO: This is scaffolding to shore up the parser logic while we're still refactoring.  Don't merge this to master
    boolean isCastableTo(ValuesSourceType valuesSourceType);
}
