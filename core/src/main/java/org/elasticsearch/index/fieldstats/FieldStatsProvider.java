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

package org.elasticsearch.index.fieldstats;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.fieldstats.FieldStats;
import org.elasticsearch.action.fieldstats.IndexConstraint;
import org.elasticsearch.action.fieldstats.IndexConstraint.Comparison;
import org.elasticsearch.action.fieldstats.IndexConstraint.Property;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.index.engine.Engine.Searcher;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.core.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.ip.IpFieldMapper.IpFieldType;
import org.joda.time.DateTimeZone;

import java.io.IOException;

/**
 * Provides a service for gettings the {@link FieldStats} for a given field from
 * the index.
 */
public class FieldStatsProvider {

    private final Searcher searcher;
    private final MapperService mapperService;

    /**
     * @param searcher
     *            the {@link Searcher}to use when retrieving the
     *            {@link FieldStats}
     * @param mapperService
     *            the {@link MapperService}
     */
    public FieldStatsProvider(Searcher searcher, MapperService mapperService) {
        this.searcher = searcher;
        this.mapperService = mapperService;
    }

    /**
     * @param field
     *            the name of the field to return {@link FieldStats} for.
     * @return a {@link FieldStats} object for the given field
     * @throws IOException
     *             if the field statistics cannot be read
     */
    public <T extends Comparable<T>> FieldStats<T> get(String field) throws IOException {
        MappedFieldType mappedFieldType = mapperService.fullName(field);
        if (mappedFieldType != null) {
                IndexReader reader = searcher.reader();
                Terms terms = MultiFields.getTerms(reader, field);
                if (terms != null) {
                    return mappedFieldType.stats(terms, reader.maxDoc());
                }
            }
        return null;
    }

    /**
     * @param fieldName
     *            the fieldName to check
     * @param from
     *            the minimum value for the query
     * @param to
     *            the maximum value for the query
     * @param includeLower
     *            whether the from value is inclusive
     * @param includeUpper
     *            whether the to value is inclusive
     * @param timeZone
     *            the timeZone to use for date fields
     * @param dateMathParser
     *            the {@link DateMathParser} to use for date fields
     * @return A {@link Relation} indicating the overlap of the range of terms
     *         for the field with the query range. This method will return:
     *         <ul>
     *         <li>{@link Relation#WITHIN} if the range of terms for the field
     *         in the shard is completely within the query range</li>
     *         <li>{@link Relation#DISJOINT} if the range of terms for the field
     *         in the shard is completely outside the query range</li>
     *         <li>{@link Relation#INTERSECTS} if the range of terms for the
     *         field in the shard intersects with the query range</li>
     *         </ul>
     * @throws IOException
     *             if the index cannot be read
     */
    public Relation isFieldWithinQuery(String fieldName, Object from, Object to, boolean includeLower, boolean includeUpper,
            DateTimeZone timeZone, DateMathParser dateMathParser) throws IOException {
        MappedFieldType mappedFieldType = mapperService.fullName(fieldName);
        FieldStats<?> fieldStats = get(fieldName);
        if (fieldStats == null) {
            // No fieldStats for the field so the field doesn't exist on
            // this shard, so relation is DISJOINT
            return Relation.DISJOINT;
        } else {
            // Convert the from and to values to Strings so they can be used
            // in the IndexConstraints. Since DateTime is represented as a
            // Long field in Lucene we need to use the millisecond value of
            // the DateTime in that case
            String fromString = null;
            if (from != null) {
                if (mappedFieldType instanceof DateFieldType) {
                    long millis = ((DateFieldType) mappedFieldType).parseToMilliseconds(from, !includeLower, timeZone, dateMathParser);
                    fromString = fieldStats.stringValueOf(millis, null);
                } else if (mappedFieldType instanceof IpFieldType) {
                    if (from instanceof BytesRef) {
                        from = ((BytesRef) from).utf8ToString();
                    }
                    long ipAsLong = ((IpFieldType) mappedFieldType).value(from);
                    fromString = fieldStats.stringValueOf(ipAsLong, null);
                } else {
                    fromString = fieldStats.stringValueOf(from, null);
                }
            }
            String toString = null;
            if (to != null) {
                if (mappedFieldType instanceof DateFieldType) {
                    long millis = ((DateFieldType) mappedFieldType).parseToMilliseconds(to, includeUpper, timeZone, dateMathParser);
                    toString = fieldStats.stringValueOf(millis, null);
                } else if (mappedFieldType instanceof IpFieldType) {
                    if (to instanceof BytesRef) {
                        to = ((BytesRef) to).utf8ToString();
                    }
                    long ipAsLong = ((IpFieldType) mappedFieldType).value(to);
                    toString = fieldStats.stringValueOf(ipAsLong, null);
                } else {
                    toString = fieldStats.stringValueOf(to, null);
                }
            }
            if ((from == null || fieldStats
                    .match(new IndexConstraint(fieldName, Property.MIN, includeLower ? Comparison.GTE : Comparison.GT, fromString)))
                    && (to == null || fieldStats.match(
                            new IndexConstraint(fieldName, Property.MAX, includeUpper ? Comparison.LTE : Comparison.LT, toString)))) {
                // If the min and max terms for the field are both within
                // the query range then all documents will match so relation is
                // WITHIN
                return Relation.WITHIN;
            } else if ((to != null && fieldStats
                    .match(new IndexConstraint(fieldName, Property.MIN, includeUpper ? Comparison.GT : Comparison.GTE, toString)))
                    || (from != null && fieldStats.match(
                            new IndexConstraint(fieldName, Property.MAX, includeLower ? Comparison.LT : Comparison.LTE, fromString)))) {
                // If the min and max terms are both outside the query range
                // then no document will match so relation is DISJOINT (N.B.
                // since from <= to we only need
                // to check one bould for each side of the query range)
                return Relation.DISJOINT;
            }
        }
        // Range of terms doesn't match any of the constraints so must INTERSECT
        return Relation.INTERSECTS;
    }

    /**
     * An enum used to describe the relation between the range of terms in a
     * shard when compared with a query range
     */
    public static enum Relation {
        WITHIN, INTERSECTS, DISJOINT;
    }
}
