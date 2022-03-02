/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.xcontent.XContentParser;

import java.util.function.Function;

/**
 * Simplified version of {@link DocumentParserContext} to be used in tests.
 * Every non final method throws {@link UnsupportedOperationException} and can be implemented as needed.
 * {@link #doc()} and {@link #path()} are defined and final, as their behaviour is standard and they
 * are both needed in almost every situation.
 * The methods defined final in {@link DocumentParserContext} depend on the provided constructor arguments.
 */
public class TestDocumentParserContext extends DocumentParserContext {
    private final LuceneDocument document = new LuceneDocument();
    private final ContentPath contentPath = new ContentPath(0);

    /**
     * The shortest and easiest way to create a context, to be used when none of the constructor arguments are needed.
     * Use with caution as it can cause {@link NullPointerException}s down the line.
     */
    public TestDocumentParserContext() {
        super(MappingLookup.EMPTY, MapperTestCase.createIndexSettings(Version.CURRENT, Settings.EMPTY), null, null, null);
    }

    /**
     * More verbose way to create a context, to be used when one or more constructor arguments are needed as final methods
     * that depend on them are called while executing tests.
     */
    public TestDocumentParserContext(
        MappingLookup mappingLookup,
        IndexSettings indexSettings,
        IndexAnalyzers indexAnalyzers,
        Function<DateFormatter, MappingParserContext> parserContextFunction,
        SourceToParse source
    ) {
        super(mappingLookup, indexSettings, indexAnalyzers, parserContextFunction, source);
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
        throw new UnsupportedOperationException();
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
