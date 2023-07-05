/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.XContentParser;

/**
 * Simplified version of {@link DocumentParserContext} to be used in tests.
 * Every non final method throws {@link UnsupportedOperationException} and can be implemented as needed.
 * {@link #doc()} and {@link #path()} are defined and final, as their behaviour is standard and they
 * are both needed in almost every situation.
 * The methods defined final in {@link DocumentParserContext} depend on the provided constructor arguments.
 */
public class TestDocumentParserContext extends DocumentParserContext {
    private final LuceneDocument document = new LuceneDocument();
    private final ContentPath contentPath = new ContentPath();
    private final XContentParser parser;

    /**
     * The shortest and easiest way to create a context, to be used when none of the constructor arguments are needed.
     * Use with caution as it can cause {@link NullPointerException}s down the line.
     */
    public TestDocumentParserContext() {
        this(MappingLookup.EMPTY, null);
    }

    public TestDocumentParserContext(XContentParser parser) {
        this(MappingLookup.EMPTY, null, parser);
    }

    /**
     * More verbose way to create a context, to be used when one or more constructor arguments are needed as final methods
     * that depend on them are called while executing tests.
     */
    public TestDocumentParserContext(MappingLookup mappingLookup, SourceToParse source) {
        this(mappingLookup, source, null);
    }

    private TestDocumentParserContext(MappingLookup mappingLookup, SourceToParse source, XContentParser parser) {
        super(
            mappingLookup,
            new MappingParserContext(
                s -> null,
                s -> null,
                s -> null,
                IndexVersion.current(),
                () -> TransportVersion.current(),
                () -> null,
                null,
                (type, name) -> Lucene.STANDARD_ANALYZER,
                MapperTestCase.createIndexSettings(IndexVersion.current(), Settings.EMPTY),
                null
            ),
            source,
            mappingLookup.getMapping().getRoot(),
            ObjectMapper.Dynamic.getRootDynamic(mappingLookup)
        );
        this.parser = parser;
    }

    @Override
    public final LuceneDocument doc() {
        return document;
    }

    @Override
    public final ContentPath path() {
        return contentPath;
    }

    @Override
    public Iterable<LuceneDocument> nonRootDocuments() {
        throw new UnsupportedOperationException();
    }

    @Override
    public XContentParser parser() {
        return parser;
    }

    @Override
    public LuceneDocument rootDoc() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void addDoc(LuceneDocument doc) {
        throw new UnsupportedOperationException();
    }
}
