/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.index.analysis.NamedAnalyzer;

/**
 * A mapper for the _id field.
 */
public abstract class IdFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_id";

    public static final String CONTENT_TYPE = "_id";

    public static final TypeParser PARSER = new FixedTypeParser(MappingParserContext::idFieldMapper);

    protected IdFieldMapper(MappedFieldType mappedFieldType, NamedAnalyzer indexAnalyzer) {
        super(mappedFieldType, indexAnalyzer);
        assert mappedFieldType.isSearchable();
    }

    @Override
    protected final String contentType() {
        return CONTENT_TYPE;
    }
}
