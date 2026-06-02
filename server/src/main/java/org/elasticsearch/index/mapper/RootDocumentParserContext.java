/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Internal version of {@link DocumentParserContext} that is aware of implementation details like nested documents
 * and how they are stored in the lucene index.
 */
class RootDocumentParserContext extends DocumentParserContext implements AutoCloseable {
    private final ContentPath path = new ContentPath();
    private final XContentParser parser;
    private final LuceneDocument document;
    private final List<LuceneDocument> documents = new ArrayList<>();
    private final long maxAllowedNumNestedDocs;
    private long numNestedDocs;
    private boolean docsReversed = false;
    private final BytesRef tsid;

    RootDocumentParserContext(
        MappingLookup mappingLookup,
        MappingParserContext mappingParserContext,
        SourceToParse source,
        XContentParser parser
    ) throws IOException {
        super(
            mappingLookup,
            mappingParserContext,
            source,
            mappingLookup.getMapping().getRoot(),
            ObjectMapper.Dynamic.getRootDynamic(mappingLookup)
        );
        IndexSettings indexSettings = mappingParserContext.getIndexSettings();
        BytesRef tsid = source.tsid();
        if (tsid == null
            && indexSettings.getMode() == IndexMode.TIME_SERIES
            && indexSettings.getIndexRouting() instanceof IndexRouting.ExtractFromSource.ForIndexDimensions forIndexDimensions) {
            // the tsid is normally set on the coordinating node during shard routing and passed to the data node via the index request
            // but when applying a translog operation, shard routing is not happening, and we have to create the tsid from source
            SourceToParse.Source sourceObject = source.source();
            // TODO: this can likely operate on eirf if present opposed to materlizing the originl source bytes if not present.
            tsid = forIndexDimensions.buildTsid(sourceObject.xContentType(), sourceObject.originalBytes());
        }
        this.tsid = tsid;
        assert this.tsid == null || indexSettings.getMode() == IndexMode.TIME_SERIES : "tsid should only be set for time series indices";
        XContentParserDecorator parserDecorator = source.getMeteringParserDecorator();
        Mapping mapping = mappingLookup.getMapping();
        if (mapping.getRoot().subobjects() == ObjectMapper.Subobjects.ENABLED) {
            this.parser = parserDecorator.decorate(DotExpandingXContentParser.expandDots(parser, this.path), mapping);
        } else {
            this.parser = parserDecorator.decorate(parser, mapping);
        }
        this.document = new LuceneDocument();
        this.documents.add(document);
        this.maxAllowedNumNestedDocs = indexSettings().getMappingNestedDocsLimit();
        this.numNestedDocs = 0L;
    }

    @Override
    public void close() throws IOException {
        parser.close();
    }

    @Override
    public final Mapper getMapper(String name) {
        return mappingLookup().getMapping().findMetadataOrRootMapper(name);
    }

    @Override
    public ContentPath path() {
        return this.path;
    }

    @Override
    public XContentParser parser() {
        return this.parser;
    }

    @Override
    public LuceneDocument rootDoc() {
        return documents.get(0);
    }

    @Override
    public LuceneDocument doc() {
        return this.document;
    }

    @Override
    protected void addDoc(LuceneDocument doc) {
        numNestedDocs++;
        if (numNestedDocs > maxAllowedNumNestedDocs) {
            throw new DocumentParsingException(
                parser.getTokenLocation(),
                "The number of nested documents has exceeded the allowed limit of ["
                    + maxAllowedNumNestedDocs
                    + "]."
                    + " This limit can be set by changing the ["
                    + MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey()
                    + "] index level setting."
            );
        }
        this.documents.add(doc);
    }

    @Override
    public BytesRef getTsid() {
        return this.tsid;
    }

    @Override
    public Iterable<LuceneDocument> nonRootDocuments() {
        if (docsReversed) {
            throw new IllegalStateException("documents are already reversed");
        }
        return documents.subList(1, documents.size());
    }

    /**
     * Returns a copy of the provided {@link List} where parent documents appear
     * after their children.
     */
    List<LuceneDocument> reorderParentAndGetDocs() {
        if (documents.size() > 1 && docsReversed == false) {
            docsReversed = true;
            // We preserve the order of the children while ensuring that parents appear after them.
            List<LuceneDocument> newDocs = new ArrayList<>(documents.size());
            LinkedList<LuceneDocument> parents = new LinkedList<>();
            for (LuceneDocument doc : documents) {
                while (parents.peek() != doc.getParent()) {
                    newDocs.add(parents.poll());
                }
                parents.add(0, doc);
            }
            newDocs.addAll(parents);
            documents.clear();
            documents.addAll(newDocs);
        }
        return documents;
    }
}
