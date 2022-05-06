/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
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

    /**
     * Description of the document being parsed used in error messages. Not
     * called unless there is an error.
     */
    public abstract String documentDescription(DocumentParserContext context);

    /**
     * Description of the document being indexed used after parsing for things
     * like version conflicts.
     */
    public abstract String documentDescription(ParsedDocument parsedDocument);

    /**
     * Create a {@link Field} to store the provided {@code _id} that "stores"
     * the {@code _id} so it can be fetched easily from the index.
     */
    public static Field standardIdField(String id) {
        return new Field(NAME, Uid.encodeId(id), ProvidedIdFieldMapper.Defaults.FIELD_TYPE);
    }
}
