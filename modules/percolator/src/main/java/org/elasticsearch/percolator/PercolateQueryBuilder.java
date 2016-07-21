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

package org.elasticsearch.percolator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static org.elasticsearch.index.mapper.SourceToParse.source;
import static org.elasticsearch.percolator.PercolatorFieldMapper.parseQuery;

public class PercolateQueryBuilder extends AbstractQueryBuilder<PercolateQueryBuilder> {
    public static final String NAME = "percolate";

    static final ParseField DOCUMENT_FIELD = new ParseField("document");
    private static final ParseField QUERY_FIELD = new ParseField("field");
    private static final ParseField DOCUMENT_TYPE_FIELD = new ParseField("document_type");
    private static final ParseField INDEXED_DOCUMENT_FIELD_INDEX = new ParseField("index");
    private static final ParseField INDEXED_DOCUMENT_FIELD_TYPE = new ParseField("type");
    private static final ParseField INDEXED_DOCUMENT_FIELD_ID = new ParseField("id");
    private static final ParseField INDEXED_DOCUMENT_FIELD_ROUTING = new ParseField("routing");
    private static final ParseField INDEXED_DOCUMENT_FIELD_PREFERENCE = new ParseField("preference");
    private static final ParseField INDEXED_DOCUMENT_FIELD_VERSION = new ParseField("version");

    private final String field;
    private final String documentType;
    private final BytesReference document;

    private final String indexedDocumentIndex;
    private final String indexedDocumentType;
    private final String indexedDocumentId;
    private final String indexedDocumentRouting;
    private final String indexedDocumentPreference;
    private final Long indexedDocumentVersion;

    public PercolateQueryBuilder(String field, String documentType, BytesReference document) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
        if (documentType == null) {
            throw new IllegalArgumentException("[document_type] is a required argument");
        }
        if (document == null) {
            throw new IllegalArgumentException("[document] is a required argument");
        }
        this.field = field;
        this.documentType = documentType;
        this.document = document;
        indexedDocumentIndex = null;
        indexedDocumentType = null;
        indexedDocumentId = null;
        indexedDocumentRouting = null;
        indexedDocumentPreference = null;
        indexedDocumentVersion = null;
    }

    public PercolateQueryBuilder(String field, String documentType, String indexedDocumentIndex, String indexedDocumentType,
                                 String indexedDocumentId, String indexedDocumentRouting, String indexedDocumentPreference,
                                 Long indexedDocumentVersion) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
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
        this.field = field;
        this.documentType = documentType;
        this.indexedDocumentIndex = indexedDocumentIndex;
        this.indexedDocumentType = indexedDocumentType;
        this.indexedDocumentId = indexedDocumentId;
        this.indexedDocumentRouting = indexedDocumentRouting;
        this.indexedDocumentPreference = indexedDocumentPreference;
        this.indexedDocumentVersion = indexedDocumentVersion;
        this.document = null;
    }

    /**
     * Read from a stream.
     */
    PercolateQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        documentType = in.readString();
        indexedDocumentIndex = in.readOptionalString();
        indexedDocumentType = in.readOptionalString();
        indexedDocumentId = in.readOptionalString();
        indexedDocumentRouting = in.readOptionalString();
        indexedDocumentPreference = in.readOptionalString();
        if (in.readBoolean()) {
            indexedDocumentVersion = in.readVLong();
        } else {
            indexedDocumentVersion = null;
        }
        document = in.readOptionalBytesReference();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
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
        out.writeOptionalBytesReference(document);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(DOCUMENT_TYPE_FIELD.getPreferredName(), documentType);
        builder.field(QUERY_FIELD.getPreferredName(), field);
        if (document != null) {
            XContentType contentType = XContentFactory.xContentType(document);
            if (contentType == builder.contentType()) {
                builder.rawField(DOCUMENT_FIELD.getPreferredName(), document);
            } else {
                try (XContentParser parser = XContentFactory.xContent(contentType).createParser(document)) {
                    parser.nextToken();
                    builder.field(DOCUMENT_FIELD.getPreferredName());
                    builder.copyCurrentStructure(parser);
                }
            }
        }
        if (indexedDocumentIndex != null || indexedDocumentType != null || indexedDocumentId != null) {
            if (indexedDocumentIndex != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_INDEX.getPreferredName(), indexedDocumentIndex);
            }
            if (indexedDocumentType != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_TYPE.getPreferredName(), indexedDocumentType);
            }
            if (indexedDocumentId != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_ID.getPreferredName(), indexedDocumentId);
            }
            if (indexedDocumentRouting != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_ROUTING.getPreferredName(), indexedDocumentRouting);
            }
            if (indexedDocumentPreference != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_PREFERENCE.getPreferredName(), indexedDocumentPreference);
            }
            if (indexedDocumentVersion != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_VERSION.getPreferredName(), indexedDocumentVersion);
            }
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<PercolateQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        String field = null;
        String documentType = null;

        String indexedDocumentIndex = null;
        String indexedDocumentType = null;
        String indexedDocumentId = null;
        String indexedDocumentRouting = null;
        String indexedDocumentPreference = null;
        Long indexedDocumentVersion = null;

        BytesReference source = null;

        String queryName = null;
        String currentFieldName = null;

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, DOCUMENT_FIELD)) {
                    try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                        builder.copyCurrentStructure(parser);
                        builder.flush();
                        source = builder.bytes();
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + PercolateQueryBuilder.NAME +
                            "] query does not support [" + token + "]");
                }
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, QUERY_FIELD)) {
                    field = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, DOCUMENT_TYPE_FIELD)) {
                    documentType = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_INDEX)) {
                    indexedDocumentIndex = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_TYPE)) {
                    indexedDocumentType = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_ID)) {
                    indexedDocumentId = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_ROUTING)) {
                    indexedDocumentRouting = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_PREFERENCE)) {
                    indexedDocumentPreference = parser.text();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, INDEXED_DOCUMENT_FIELD_VERSION)) {
                    indexedDocumentVersion = parser.longValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + PercolateQueryBuilder.NAME +
                            "] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[" + PercolateQueryBuilder.NAME +
                        "] query does not support [" + token + "]");
            }
        }

        if (documentType == null) {
            throw new IllegalArgumentException("[" + PercolateQueryBuilder.NAME + "] query is missing required [" +
                    DOCUMENT_TYPE_FIELD.getPreferredName() + "] parameter");
        }

        PercolateQueryBuilder queryBuilder;
        if (source != null) {
            queryBuilder = new PercolateQueryBuilder(field, documentType, source);
        } else if (indexedDocumentId != null) {
            queryBuilder = new PercolateQueryBuilder(field, documentType, indexedDocumentIndex, indexedDocumentType,
                    indexedDocumentId, indexedDocumentRouting, indexedDocumentPreference, indexedDocumentVersion);
        } else {
            throw new IllegalArgumentException("[" + PercolateQueryBuilder.NAME + "] query, nothing to percolate");
        }
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return Optional.of(queryBuilder);
    }

    @Override
    protected boolean doEquals(PercolateQueryBuilder other) {
        return Objects.equals(field, other.field)
                && Objects.equals(documentType, other.documentType)
                && Objects.equals(document, other.document)
                && Objects.equals(indexedDocumentIndex, other.indexedDocumentIndex)
                && Objects.equals(indexedDocumentType, other.indexedDocumentType)
                && Objects.equals(indexedDocumentId, other.indexedDocumentId);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, documentType, document, indexedDocumentIndex, indexedDocumentType, indexedDocumentId);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
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
        if(getResponse.isSourceEmpty()) {
            throw new IllegalArgumentException(
                "indexed document [" + indexedDocumentIndex + "/" + indexedDocumentType + "/" + indexedDocumentId + "] source disabled"
            );
        }
        return new PercolateQueryBuilder(field, documentType, getResponse.getSourceAsBytesRef());
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (indexedDocumentIndex != null || indexedDocumentType != null || indexedDocumentId != null) {
            throw new IllegalStateException("query builder must be rewritten first");
        }

        if (document == null) {
            throw new IllegalStateException("no document to percolate");
        }

        MapperService mapperService = context.getMapperService();
        DocumentMapperForType docMapperForType = mapperService.documentMapperWithAutoCreate(documentType);
        DocumentMapper docMapper = docMapperForType.getDocumentMapper();

        ParsedDocument doc = docMapper.parse(source(context.index().getName(), documentType, "_temp_id", document));

        FieldNameAnalyzer fieldNameAnalyzer = (FieldNameAnalyzer) docMapper.mappers().indexAnalyzer();
        // Need to this custom impl because FieldNameAnalyzer is strict and the percolator sometimes isn't when
        // 'index.percolator.map_unmapped_fields_as_string' is enabled:
        Analyzer analyzer = new DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
            @Override
            protected Analyzer getWrappedAnalyzer(String fieldName) {
                Analyzer analyzer = fieldNameAnalyzer.analyzers().get(fieldName);
                if (analyzer != null) {
                    return analyzer;
                } else {
                    return context.getAnalysisService().defaultIndexAnalyzer();
                }
            }
        };
        final IndexSearcher docSearcher;
        if (doc.docs().size() > 1) {
            assert docMapper.hasNestedObjects();
            docSearcher = createMultiDocumentSearcher(analyzer, doc);
        } else {
            MemoryIndex memoryIndex = MemoryIndex.fromDocument(doc.rootDoc(), analyzer, true, false);
            docSearcher = memoryIndex.createSearcher();
            docSearcher.setQueryCache(null);
        }

        Version indexVersionCreated = context.getIndexSettings().getIndexVersionCreated();
        boolean mapUnmappedFieldsAsString = context.getIndexSettings()
                .getValue(PercolatorFieldMapper.INDEX_MAP_UNMAPPED_FIELDS_AS_STRING_SETTING);
        if (indexVersionCreated.onOrAfter(Version.V_5_0_0_alpha1)) {
            MappedFieldType fieldType = context.fieldMapper(field);
            if (fieldType == null) {
                throw new QueryShardException(context, "field [" + field + "] does not exist");
            }

            if (!(fieldType instanceof PercolatorFieldMapper.FieldType)) {
                throw new QueryShardException(context, "expected field [" + field +
                        "] to be of type [percolator], but is of type [" + fieldType.typeName() + "]");
            }
            PercolatorFieldMapper.FieldType pft = (PercolatorFieldMapper.FieldType) fieldType;
            PercolateQuery.QueryStore queryStore = createStore(pft, context, mapUnmappedFieldsAsString);
            return pft.percolateQuery(documentType, queryStore, document, docSearcher);
        } else {
            Query percolateTypeQuery = new TermQuery(new Term(TypeFieldMapper.NAME, MapperService.PERCOLATOR_LEGACY_TYPE_NAME));
            PercolateQuery.QueryStore queryStore = createLegacyStore(context, mapUnmappedFieldsAsString);
            return new PercolateQuery(documentType, queryStore, document, percolateTypeQuery, docSearcher,
                    new MatchNoDocsQuery("pre 5.0.0-alpha1 index, no verified matches"));
        }
    }

    public String getField() {
        return field;
    }

    public String getDocumentType() {
        return documentType;
    }

    public BytesReference getDocument() {
        return document;
    }

    static IndexSearcher createMultiDocumentSearcher(Analyzer analyzer, ParsedDocument doc) {
        RAMDirectory ramDirectory = new RAMDirectory();
        try (IndexWriter indexWriter = new IndexWriter(ramDirectory, new IndexWriterConfig(analyzer))) {
            indexWriter.addDocuments(doc.docs());
            indexWriter.commit();
            DirectoryReader directoryReader = DirectoryReader.open(ramDirectory);
            assert directoryReader.leaves().size() == 1 : "Expected single leaf, but got [" + directoryReader.leaves().size() + "]";
            final IndexSearcher slowSearcher = new IndexSearcher(directoryReader) {

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

    private static PercolateQuery.QueryStore createStore(PercolatorFieldMapper.FieldType fieldType,
                                                         QueryShardContext context,
                                                         boolean mapUnmappedFieldsAsString) {
        return ctx -> {
            LeafReader leafReader = ctx.reader();
            BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(fieldType.queryBuilderField.name());
            if (binaryDocValues == null) {
                return docId -> null;
            }

            Bits bits = leafReader.getDocsWithField(fieldType.queryBuilderField.name());
            return docId -> {
                if (bits.get(docId)) {
                    BytesRef qbSource = binaryDocValues.get(docId);
                    if (qbSource.length > 0) {
                        XContent xContent = PercolatorFieldMapper.QUERY_BUILDER_CONTENT_TYPE.xContent();
                        try (XContentParser sourceParser = xContent.createParser(qbSource.bytes, qbSource.offset, qbSource.length)) {
                            return parseQuery(context, mapUnmappedFieldsAsString, sourceParser);
                        }
                    } else {
                        return null;
                    }
                } else {
                    return null;
                }
            };
        };
    }

    private static PercolateQuery.QueryStore createLegacyStore(QueryShardContext context, boolean mapUnmappedFieldsAsString) {
        return ctx -> {
            LeafReader leafReader = ctx.reader();
            return docId -> {
                LegacyQueryFieldVisitor visitor = new LegacyQueryFieldVisitor();
                leafReader.document(docId, visitor);
                if (visitor.source == null) {
                    throw new IllegalStateException("No source found for document with docid [" + docId + "]");
                }

                try (XContentParser sourceParser = XContentHelper.createParser(visitor.source)) {
                    String currentFieldName = null;
                    XContentParser.Token token = sourceParser.nextToken(); // move the START_OBJECT
                    if (token != XContentParser.Token.START_OBJECT) {
                        throw new ElasticsearchException("failed to parse query [" + docId + "], not starting with OBJECT");
                    }
                    while ((token = sourceParser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = sourceParser.currentName();
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if ("query".equals(currentFieldName)) {
                                return parseQuery(context, mapUnmappedFieldsAsString, sourceParser);
                            } else {
                                sourceParser.skipChildren();
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            sourceParser.skipChildren();
                        }
                    }
                }
                return null;
            };
        };
    }

    private static final class LegacyQueryFieldVisitor extends StoredFieldVisitor {

        private BytesArray source;

        @Override
        public void binaryField(FieldInfo fieldInfo, byte[] bytes) throws IOException {
            source = new BytesArray(bytes);
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (source != null)  {
                return Status.STOP;
            }
            if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
                return Status.YES;
            } else {
                return Status.NO;
            }
        }

    }

}
