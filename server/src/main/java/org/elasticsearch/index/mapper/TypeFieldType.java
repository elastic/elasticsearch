/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.logging.DeprecationCategory;
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
        deprecationLogger.deprecate(DeprecationCategory.QUERIES, "typefieldtype", TYPES_V7_DEPRECATION_MESSAGE);
        return new MatchAllDocsQuery();
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        deprecationLogger.deprecate(DeprecationCategory.QUERIES, "typefieldtype", TYPES_V7_DEPRECATION_MESSAGE);
        return new ConstantIndexFieldData.Builder(type, name(), CoreValuesSourceType.KEYWORD);
    }

    @Override
    public ValueFetcher valueFetcher(SearchExecutionContext context, String format) {
        throw new UnsupportedOperationException("Cannot fetch values for internal field [" + name() + "].");
    }

    @Override
    protected boolean matches(String pattern, boolean caseInsensitive, SearchExecutionContext context) {
        deprecationLogger.deprecate(DeprecationCategory.QUERIES, "typefieldtype", TYPES_V7_DEPRECATION_MESSAGE);
        if (caseInsensitive) {
            return pattern.equalsIgnoreCase(type);
        }
        return pattern.equals(type);
    }
}
