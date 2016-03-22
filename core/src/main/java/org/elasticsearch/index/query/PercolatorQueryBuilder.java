/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.percolator.PercolatorFieldMapper;
import org.elasticsearch.index.percolator.PercolatorQueryCache;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.index.mapper.SourceToParse.source;

public class PercolatorQueryBuilder extends AbstractQueryBuilder<PercolatorQueryBuilder> {

    public static final String NAME = "percolator";
    static final PercolatorQueryBuilder PROTO = new PercolatorQueryBuilder(null, null, null, null, null, null, null, null);

    private final String documentType;
    private final BytesReference document;

    private final String indexedDocumentIndex;
    private final String indexedDocumentType;
    private final String indexedDocumentId;
    private final String indexedDocumentRouting;
    private final String indexedDocumentPreference;
    private final Long indexedDocumentVersion;

    public PercolatorQueryBuilder(String documentType, BytesReference document) {
        if (documentType == null) {
            throw new IllegalArgumentException("[document_type] is a required argument");
        }
        if (document == null) {
            throw new IllegalArgumentException("[document] is a required argument");
        }
        this.documentType = documentType;
        this.document = document;
        indexedDocumentIndex = null;
        indexedDocumentType = null;
        indexedDocumentId = null;
        indexedDocumentRouting = null;
        indexedDocumentPreference = null;
        indexedDocumentVersion = null;
    }

    public PercolatorQueryBuilder(String documentType, String indexedDocumentIndex, String indexedDocumentType,
                                  String indexedDocumentId, String indexedDocumentRouting, String indexedDocumentPreference,
                                  Long indexedDocumentVersion) {
        if (documentType == null) {
            throw new IllegalArgumentException("[document_type] is a required argument");
        }
        if (indexedDocumentIndex == null) {
            throw new IllegalArgumentException("[index] is a required argument");
        }
        if (indexedDocumentType == null) {
            throw new IllegalArgumentException("[type] is a required argument");
        }
        if (indexedDocumentId == null) {
            throw new IllegalArgumentException("[id] is a required argument");
        }
        this.documentType = documentType;
        this.indexedDocumentIndex = indexedDocumentIndex;
        this.indexedDocumentType = indexedDocumentType;
        this.indexedDocumentId = indexedDocumentId;
        this.indexedDocumentRouting = indexedDocumentRouting;
        this.indexedDocumentPreference = indexedDocumentPreference;
        this.indexedDocumentVersion = indexedDocumentVersion;
        this.document = null;
    }

    private PercolatorQueryBuilder(String documentType, BytesReference document, String indexedDocumentIndex, String indexedDocumentType,
                                   String indexedDocumentId, String indexedDocumentRouting, String indexedDocumentPreference,
                                   Long indexedDocumentVersion) {
        this.documentType = documentType;
        this.document = document;
        this.indexedDocumentIndex = indexedDocumentIndex;
        this.indexedDocumentType = indexedDocumentType;
        this.indexedDocumentId = indexedDocumentId;
        this.indexedDocumentRouting = indexedDocumentRouting;
        this.indexedDocumentPreference = indexedDocumentPreference;
        this.indexedDocumentVersion = indexedDocumentVersion;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(PercolatorQueryParser.DOCUMENT_TYPE_FIELD.getPreferredName(), documentType);
        if (document != null) {
            XContentType contentType = XContentFactory.xContentType(document);
            if (contentType == builder.contentType()) {
                builder.rawField(PercolatorQueryParser.DOCUMENT_FIELD.getPreferredName(), document);
            } else {
                XContentParser parser = XContentFactory.xContent(contentType).createParser(document);
                parser.nextToken();
                builder.field(PercolatorQueryParser.DOCUMENT_FIELD.getPreferredName());
                builder.copyCurrentStructure(parser);
            }
        }
        if (indexedDocumentIndex != null || indexedDocumentType != null || indexedDocumentId != null) {
            if (indexedDocumentIndex != null) {
                builder.field(PercolatorQueryParser.INDEXED_DOCUMENT_FIELD_INDEX.getPreferredName(), indexedDocumentIndex);
            }
            if (indexedDocumentType != null) {
                builder.field(PercolatorQueryParser.INDEXED_DOCUMENT_FIELD_TYPE.getPreferredName(), indexedDocumentType);
            }
            if (indexedDocumentId != null) {
                builder.field(PercolatorQueryParser.INDEXED_DOCUMENT_FIELD_ID.getPreferredName(), indexedDocumentId);
            }
            if (indexedDocumentRouting != null) {
                builder.field(PercolatorQueryParser.INDEXED_DOCUMENT_FIELD_ROUTING.getPreferredName(), indexedDocumentRouting);
            }
            if (indexedDocumentPreference != null) {
                builder.field(PercolatorQueryParser.INDEXED_DOCUMENT_FIELD_PREFERENCE.getPreferredName(), indexedDocumentPreference);
            }
            if (indexedDocumentVersion != null) {
                builder.field(PercolatorQueryParser.INDEXED_DOCUMENT_FIELD_VERSION.getPreferredName(), indexedDocumentVersion);
            }
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected PercolatorQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String docType = in.readString();
        String documentIndex = in.readOptionalString();
        String documentType = in.readOptionalString();
        String documentId = in.readOptionalString();
        String documentRouting = in.readOptionalString();
        String documentPreference = in.readOptionalString();
        Long documentVersion = null;
        if (in.readBoolean()) {
            documentVersion = in.readVLong();
        }
        BytesReference documentSource = null;
        if (in.readBoolean()) {
            documentSource = in.readBytesReference();
        }
        return new PercolatorQueryBuilder(docType, documentSource, documentIndex, documentType, documentId,
                documentRouting, documentPreference, documentVersion);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(documentType);
        out.writeOptionalString(indexedDocumentIndex);
        out.writeOptionalString(indexedDocumentType);
        out.writeOptionalString(indexedDocumentId);
        out.writeOptionalString(indexedDocumentRouting);
        out.writeOptionalString(indexedDocumentPreference);
        if (indexedDocumentVersion != null) {
            out.writeBoolean(true);
            out.writeVLong(indexedDocumentVersion);
        } else {
            out.writeBoolean(false);
        }
        if (document != null) {
            out.writeBoolean(true);
            out.writeBytesReference(document);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    protected boolean doEquals(PercolatorQueryBuilder other) {
        return Objects.equals(documentType, other.documentType)
                && Objects.equals(document, other.document)
                && Objects.equals(indexedDocumentIndex, other.indexedDocumentIndex)
                && Objects.equals(indexedDocumentType, other.indexedDocumentType)
                && Objects.equals(indexedDocumentId, other.indexedDocumentId);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(documentType, document, indexedDocumentIndex, indexedDocumentType, indexedDocumentId);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder<?> doRewrite(QueryRewriteContext queryShardContext) throws IOException {
        if (document != null) {
            return this;
        }

        GetRequest getRequest = new GetRequest(indexedDocumentIndex, indexedDocumentType, indexedDocumentId);
        getRequest.preference("_local");
        getRequest.routing(indexedDocumentRouting);
        getRequest.preference(indexedDocumentPreference);
        if (indexedDocumentVersion != null) {
            getRequest.version(indexedDocumentVersion);
        }
        GetResponse getResponse = queryShardContext.getClient().get(getRequest).actionGet();
        if (getResponse.isExists() == false) {
            throw new ResourceNotFoundException(
                    "indexed document [{}/{}/{}] couldn't be found", indexedDocumentIndex, indexedDocumentType, indexedDocumentId
            );
        }
        return new PercolatorQueryBuilder(documentType, getResponse.getSourceAsBytesRef());
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (indexedDocumentIndex != null || indexedDocumentType != null || indexedDocumentId != null) {
            throw new IllegalStateException("query builder must be rewritten first");
        }

        if (document == null) {
            throw new IllegalStateException("nothing to percolator");
        }

        MapperService mapperService = context.getMapperService();
        DocumentMapperForType docMapperForType = mapperService.documentMapperWithAutoCreate(documentType);
        DocumentMapper docMapper = docMapperForType.getDocumentMapper();

        ParsedDocument doc = docMapper.parse(source(document)
            .index(context.index().getName())
            .id("_temp_id")
            .type(documentType));

        Analyzer defaultAnalyzer = context.getAnalysisService().defaultIndexAnalyzer();
        final IndexSearcher docSearcher;
        if (doc.docs().size() > 1) {
            assert docMapper.hasNestedObjects();
            docSearcher = createMultiDocumentSearcher(docMapper, defaultAnalyzer, doc);
        } else {
            // TODO: we may want to bring to MemoryIndex thread local cache back...
            // but I'm unsure about the real benefits.
            MemoryIndex memoryIndex = new MemoryIndex(true);
            indexDoc(docMapper, defaultAnalyzer, doc.rootDoc(), memoryIndex);
            docSearcher = memoryIndex.createSearcher();
            docSearcher.setQueryCache(null);
        }

        PercolatorQueryCache registry = context.getPercolatorQueryCache();
        if (registry == null) {
            throw new QueryShardException(context, "no percolator query registry");
        }

        Query percolateTypeQuery = new TermQuery(new Term(TypeFieldMapper.NAME, PercolatorFieldMapper.TYPE_NAME));
        PercolatorQuery.Builder builder = new PercolatorQuery.Builder(
                documentType, registry, document, docSearcher, percolateTypeQuery
        );
        Settings indexSettings = registry.getIndexSettings().getSettings();
        if (indexSettings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null).onOrAfter(Version.V_5_0_0)) {
            builder.extractQueryTermsQuery(
                    PercolatorFieldMapper.EXTRACTED_TERMS_FULL_FIELD_NAME, PercolatorFieldMapper.UNKNOWN_QUERY_FULL_FIELD_NAME
            );
        }
        return builder.build();
    }

    public String getDocumentType() {
        return documentType;
    }

    public BytesReference getDocument() {
        return document;
    }

    private IndexSearcher createMultiDocumentSearcher(DocumentMapper docMapper, Analyzer defaultAnalyzer, ParsedDocument doc) {
        IndexReader[] memoryIndices = new IndexReader[doc.docs().size()];
        List<ParseContext.Document> docs = doc.docs();
        int rootDocIndex = docs.size() - 1;
        assert rootDocIndex > 0;
        for (int i = 0; i < docs.size(); i++) {
            ParseContext.Document d = docs.get(i);
            MemoryIndex memoryIndex = new MemoryIndex(true);
            indexDoc(docMapper, defaultAnalyzer, d, memoryIndex);
            memoryIndices[i] = memoryIndex.createSearcher().getIndexReader();
        }
        try {
            MultiReader mReader = new MultiReader(memoryIndices, true);
            LeafReader slowReader = SlowCompositeReaderWrapper.wrap(mReader);
            final IndexSearcher slowSearcher = new IndexSearcher(slowReader) {

                @Override
                public Weight createNormalizedWeight(Query query, boolean needsScores) throws IOException {
                    BooleanQuery.Builder bq = new BooleanQuery.Builder();
                    bq.add(query, BooleanClause.Occur.MUST);
                    bq.add(Queries.newNestedFilter(), BooleanClause.Occur.MUST_NOT);
                    return super.createNormalizedWeight(bq.build(), needsScores);
                }

            };
            slowSearcher.setQueryCache(null);
            return slowSearcher;
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to create index for percolator with nested document ", e);
        }
    }

    private void indexDoc(DocumentMapper documentMapper, Analyzer defaultAnalyzer, ParseContext.Document document,
                          MemoryIndex memoryIndex) {
        for (IndexableField field : document.getFields()) {
            if (field.fieldType().indexOptions() == IndexOptions.NONE && field.name().equals(UidFieldMapper.NAME)) {
                continue;
            }

            Analyzer analyzer = defaultAnalyzer;
            if (documentMapper != null && documentMapper.mappers().getMapper(field.name()) != null) {
                analyzer =  documentMapper.mappers().indexAnalyzer();
            }
            try {
                try (TokenStream tokenStream = field.tokenStream(analyzer, null)) {
                    if (tokenStream != null) {
                        memoryIndex.addField(field.name(), tokenStream, field.boost());
                    }
                }
            } catch (IOException e) {
                throw new ElasticsearchException("Failed to create token stream", e);
            }
        }
    }

}
