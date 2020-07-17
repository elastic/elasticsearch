/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.TextSearchInfo;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * Abstract base {@linkplain MappedFieldType} for scripted fields.
 */
abstract class AbstractScriptMappedFieldType extends MappedFieldType {
    protected final Script script;

    AbstractScriptMappedFieldType(String name, Script script, Map<String, String> meta) {
        super(name, false, false, TextSearchInfo.NONE, meta);
        this.script = script;
    }

    protected abstract String runtimeType();

    void mapperXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("runtime_type", runtimeType());
        builder.field("script", script, params);
    }

    @Override
    public final String typeName() {
        return ScriptFieldMapper.CONTENT_TYPE;
    }

    @Override
    public final String familyTypeName() {
        return runtimeType();
    }

    @Override
    public final boolean isSearchable() {
        return true;
    }

    @Override
    public final boolean isAggregatable() {
        return true;
    }

    protected final void checkAllowExpensiveQueries(QueryShardContext context) {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "queries cannot be executed against ["
                    + ScriptFieldMapper.CONTENT_TYPE
                    + "] fields while ["
                    + ALLOW_EXPENSIVE_QUERIES.getKey()
                    + "] is set to [false]."
            );
        }
    }
}
