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

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.plain.ConstantIndexFieldData;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.Collections;
import java.util.function.Supplier;

/**
 * Mediates access to the deprecated _type field
 */
public final class TypeFieldType extends ConstantFieldType {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TypeFieldType.class);
    public static final String TYPES_V7_DEPRECATION_MESSAGE = "[types removal] Using the _type field " +
        "in queries and aggregations is deprecated, prefer to use a field instead.";

    public static final String NAME = "_type";

    public static final String CONTENT_TYPE = "_type";

    private final String type;

    TypeFieldType(String type) {
        super(NAME, Collections.emptyMap());
        this.type = type;
    }

    /**
     * Returns the name of the current type
     */
    public String getType() {
        return type;
    }

    @Override
    public String typeName() {
        return CONTENT_TYPE;
    }

    @Override
    public Query existsQuery(SearchExecutionContext context) {
        deprecationLogger.deprecate("typefieldtype", TYPES_V7_DEPRECATION_MESSAGE);
        return new MatchAllDocsQuery();
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        deprecationLogger.deprecate("typefieldtype", TYPES_V7_DEPRECATION_MESSAGE);
        return new ConstantIndexFieldData.Builder(type, name(), CoreValuesSourceType.KEYWORD);
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
    }

    @Override
    protected boolean matches(String pattern, boolean caseInsensitive, SearchExecutionContext context) {
        deprecationLogger.deprecate("typefieldtype", TYPES_V7_DEPRECATION_MESSAGE);
        if (caseInsensitive) {
            return pattern.equalsIgnoreCase(type);
        }
        return pattern.equals(type);
    }
}
