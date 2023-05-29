/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.query.support.NestedScope;

public class PercolatorExecutionContext extends SearchExecutionContext {

    protected boolean allowUnmappedFields;
    protected boolean mapUnmappedFieldAsString;

    public PercolatorExecutionContext(final SearchExecutionContext source, boolean allowUnmappedFields, boolean mapUnmappedFieldAsString) {
        super(source);
        this.allowUnmappedFields = allowUnmappedFields;
        this.mapUnmappedFieldAsString = mapUnmappedFieldAsString;
    }

    @Override
    MappedFieldType failIfFieldMappingNotFound(String name, MappedFieldType fieldMapping) {
        if (fieldMapping != null || allowUnmappedFields) {
            return fieldMapping;
        } else if (mapUnmappedFieldAsString) {
            TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, getIndexAnalyzers());
            return builder.build(MapperBuilderContext.root(false)).fieldType();
        } else {
            throw new QueryShardException(this, "No field mapping can be found for the field with name [{}]", name);
        }
    }

    @Override
    protected void reset() {
        this.allowUnmappedFields = indexSettings.isDefaultAllowUnmappedFields();
        this.lookup = null;
        this.namedQueries.clear();
        this.nestedScope = new NestedScope();
    }
}
