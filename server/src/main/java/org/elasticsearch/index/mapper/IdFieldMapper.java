/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.analysis.NamedAnalyzer;

import java.util.Map;

/**
 * A mapper for the _id field.
 */
public abstract class IdFieldMapper extends MetadataFieldMapper {
    public static final String NAME = "_id";

    public static final String CONTENT_TYPE = "_id";

    public static final TypeParser PARSER = new FixedTypeParser(MappingParserContext::idFieldMapper);

    private static final Map<String, NamedAnalyzer> ANALYZERS = Map.of(NAME, Lucene.KEYWORD_ANALYZER);

    protected IdFieldMapper(MappedFieldType mappedFieldType) {
        super(mappedFieldType);
        assert mappedFieldType.isSearchable();
    }

    @Override
    public Map<String, NamedAnalyzer> indexAnalyzers() {
        return ANALYZERS;
    }

    @Override
    protected final String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public final SourceLoader.SyntheticFieldLoader syntheticFieldLoader() {
        return SourceLoader.SyntheticFieldLoader.NOTHING;
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
     * Build the {@code _id} to use on requests reindexing into indices using
     * this {@code _id}.
     */
    public abstract String reindexId(String id);

    /**
     * Create a {@link Field} to store the provided {@code _id} that "stores"
     * the {@code _id} so it can be fetched easily from the index.
     */
    public static Field standardIdField(String id) {
        return new StringField(NAME, Uid.encodeId(id), Field.Store.YES);
    }
}
