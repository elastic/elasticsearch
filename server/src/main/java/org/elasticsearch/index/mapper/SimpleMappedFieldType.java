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

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.joda.time.DateTimeZone;

/**
 * {@link MappedFieldType} base impl for field types that are neither dates nor ranges.
 */
public abstract class SimpleMappedFieldType extends MappedFieldType {

    protected SimpleMappedFieldType() {
        super();
    }

    protected SimpleMappedFieldType(MappedFieldType ref) {
        super(ref);
    }

    @Override
    public final Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
            ShapeRelation relation, DateTimeZone timeZone, DateMathParser parser, QueryShardContext context) {
        if (relation == ShapeRelation.DISJOINT) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() +
                    "] does not support DISJOINT ranges");
        }
        // We do not fail on non-null time zones and date parsers
        // The reasoning is that on query parsers, you might want to set a time zone or format for date fields
        // but then the API has no way to know which fields are dates and which fields are not dates
        return rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, context);
    }

    /**
     * Same as {@link #rangeQuery(Object, Object, boolean, boolean, ShapeRelation, DateTimeZone, DateMathParser, QueryShardContext)}
     * but without the trouble of relations or date-specific options.
     */
    protected Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper,
            QueryShardContext context) {
        throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support range queries");
    }

}
