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
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.FieldNameAnalyzer;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperForType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.index.mapper.SourceToParse.source;
import static org.elasticsearch.percolator.PercolatorFieldMapper.parseQuery;

public class PercolateQueryBuilder extends AbstractQueryBuilder<PercolateQueryBuilder> {
    public static final String NAME = "percolate";

    private static final DeprecationLogger DEPRECATION_LOGGER = new DeprecationLogger(Loggers.getLogger(ParseField.class));

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
    @Deprecated
    private final String documentType;
    private final BytesReference document;
    private final XContentType documentXContentType;

    private final String indexedDocumentIndex;
    private final String indexedDocumentType;
    private final String indexedDocumentId;
    private final String indexedDocumentRouting;
    private final String indexedDocumentPreference;
    private final Long indexedDocumentVersion;
    private final Supplier<BytesReference> documentSupplier;

    /**
     * @deprecated use {@link #PercolateQueryBuilder(String, BytesReference, XContentType)} with the document content
     * type to avoid autodetection.
     */
    @Deprecated
    public PercolateQueryBuilder(String field, String documentType, BytesReference document) {
        this(field, documentType, document, XContentFactory.xContentType(document));
    }

    /**
     * Creates a percolator query builder instance for percolating a provided document.
     *
     * @param field                     The field that contains the percolator query
     * @param document                  The binary blob containing document to percolate
     * @param documentXContentType      The content type of the binary blob containing the document to percolate
     */
    public PercolateQueryBuilder(String field, BytesReference document, XContentType documentXContentType) {
        this(field, null, document, documentXContentType);
    }

    @Deprecated
    public PercolateQueryBuilder(String field, String documentType, BytesReference document, XContentType documentXContentType) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
        if (document == null) {
            throw new IllegalArgumentException("[document] is a required argument");
        }
        this.field = field;
        this.documentType = documentType;
        this.document = document;
        this.documentXContentType = Objects.requireNonNull(documentXContentType);
        indexedDocumentIndex = null;
        indexedDocumentType = null;
        indexedDocumentId = null;
        indexedDocumentRouting = null;
        indexedDocumentPreference = null;
        indexedDocumentVersion = null;
        this.documentSupplier = null;
    }

    private PercolateQueryBuilder(String field, String documentType, Supplier<BytesReference> documentSupplier) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
        this.field = field;
        this.documentType = documentType;
        this.document = null;
        this.documentXContentType = null;
        this.documentSupplier = documentSupplier;
        indexedDocumentIndex = null;
        indexedDocumentType = null;
        indexedDocumentId = null;
        indexedDocumentRouting = null;
        indexedDocumentPreference = null;
        indexedDocumentVersion = null;
    }

    /**
     * Creates a percolator query builder instance for percolating a document in a remote index.
     *
     * @param field                     The field that contains the percolator query
     * @param indexedDocumentIndex      The index containing the document to percolate
     * @param indexedDocumentType       The type containing the document to percolate
     * @param indexedDocumentId         The id of the document to percolate
     * @param indexedDocumentRouting    The routing value for the document to percolate
     * @param indexedDocumentPreference The preference to use when fetching the document to percolate
     * @param indexedDocumentVersion    The expected version of the document to percolate
     */
    public PercolateQueryBuilder(String field, String indexedDocumentIndex, String indexedDocumentType, String indexedDocumentId,
                                 String indexedDocumentRouting, String indexedDocumentPreference, Long indexedDocumentVersion) {
        this(field, null, indexedDocumentIndex, indexedDocumentType, indexedDocumentId, indexedDocumentRouting,
            indexedDocumentPreference, indexedDocumentVersion);
    }

    @Deprecated
    public PercolateQueryBuilder(String field, String documentType, String indexedDocumentIndex,
                                 String indexedDocumentType, String indexedDocumentId, String indexedDocumentRouting,
                                 String indexedDocumentPreference, Long indexedDocumentVersion) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
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
        this.documentXContentType = null;
        this.documentSupplier = null;
    }

    /**
     * Read from a stream.
     */
    PercolateQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        if (in.getVersion().before(Version.V_6_0_0_beta1)) {
            documentType = in.readString();
        } else {
            documentType = in.readOptionalString();
        }
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
        if (document != null) {
            if (in.getVersion().onOrAfter(Version.V_5_3_0)) {
                documentXContentType = XContentType.readFrom(in);
            } else {
                documentXContentType = XContentFactory.xContentType(document);
            }
        } else {
            documentXContentType = null;
        }
        documentSupplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (documentSupplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(field);
        if (out.getVersion().before(Version.V_6_0_0_beta1)) {
            out.writeString(documentType);
        } else {
            out.writeOptionalString(documentType);
        }
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
        if (document != null && out.getVersion().onOrAfter(Version.V_5_3_0)) {
            documentXContentType.writeTo(out);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(DOCUMENT_TYPE_FIELD.getPreferredName(), documentType);
        builder.field(QUERY_FIELD.getPreferredName(), field);
        if (document != null) {
            builder.rawField(DOCUMENT_FIELD.getPreferredName(), document);
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

    public static PercolateQueryBuilder fromXContent(XContentParser parser) throws IOException {
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
                if (DOCUMENT_FIELD.match(currentFieldName)) {
                    try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                        builder.copyCurrentStructure(parser);
                        builder.flush();
                        source = builder.bytes();
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[" + PercolateQueryBuilder.NAME +
                            "] query does not support [" + token + "]");
                }
            } else if (token.isValue() || token == XContentParser.Token.VALUE_NULL) {
                if (QUERY_FIELD.match(currentFieldName)) {
                    field = parser.text();
                } else if (DOCUMENT_TYPE_FIELD.match(currentFieldName)) {
                    documentType = parser.textOrNull();
                } else if (INDEXED_DOCUMENT_FIELD_INDEX.match(currentFieldName)) {
                    indexedDocumentIndex = parser.text();
                } else if (INDEXED_DOCUMENT_FIELD_TYPE.match(currentFieldName)) {
                    indexedDocumentType = parser.text();
                } else if (INDEXED_DOCUMENT_FIELD_ID.match(currentFieldName)) {
                    indexedDocumentId = parser.text();
                } else if (INDEXED_DOCUMENT_FIELD_ROUTING.match(currentFieldName)) {
                    indexedDocumentRouting = parser.text();
                } else if (INDEXED_DOCUMENT_FIELD_PREFERENCE.match(currentFieldName)) {
                    indexedDocumentPreference = parser.text();
                } else if (INDEXED_DOCUMENT_FIELD_VERSION.match(currentFieldName)) {
                    indexedDocumentVersion = parser.longValue();
                } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName)) {
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

        PercolateQueryBuilder queryBuilder;
        if (source != null) {
            queryBuilder = new PercolateQueryBuilder(field, documentType, source, XContentType.JSON);
        } else if (indexedDocumentId != null) {
            queryBuilder = new PercolateQueryBuilder(field, documentType, indexedDocumentIndex, indexedDocumentType,
                    indexedDocumentId, indexedDocumentRouting, indexedDocumentPreference, indexedDocumentVersion);
        } else {
            throw new IllegalArgumentException("[" + PercolateQueryBuilder.NAME + "] query, nothing to percolate");
        }
        queryBuilder.queryName(queryName);
        queryBuilder.boost(boost);
        return queryBuilder;
    }

    @Override
    protected boolean doEquals(PercolateQueryBuilder other) {
        return Objects.equals(field, other.field)
                && Objects.equals(documentType, other.documentType)
                && Objects.equals(document, other.document)
                && Objects.equals(indexedDocumentIndex, other.indexedDocumentIndex)
                && Objects.equals(indexedDocumentType, other.indexedDocumentType)
                && Objects.equals(documentSupplier, other.documentSupplier)
                && Objects.equals(indexedDocumentId, other.indexedDocumentId);

    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, documentType, document, indexedDocumentIndex, indexedDocumentType, indexedDocumentId, documentSupplier);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) {
        if (document != null) {
            return this;
        } else if (documentSupplier != null) {
            final BytesReference source = documentSupplier.get();
            if (source == null) {
                return this; // not executed yet
            } else {
                return new PercolateQueryBuilder(field, documentType, source, XContentFactory.xContentType(source));
            }
        }
        GetRequest getRequest = new GetRequest(indexedDocumentIndex, indexedDocumentType, indexedDocumentId);
        getRequest.preference("_local");
        getRequest.routing(indexedDocumentRouting);
        getRequest.preference(indexedDocumentPreference);
        if (indexedDocumentVersion != null) {
            getRequest.version(indexedDocumentVersion);
        }
        SetOnce<BytesReference> documentSupplier = new SetOnce<>();
        queryShardContext.registerAsyncAction((client, listener) -> {
            client.get(getRequest, ActionListener.wrap(getResponse -> {
                if (getResponse.isExists() == false) {
                    throw new ResourceNotFoundException(
                        "indexed document [{}/{}/{}] couldn't be found", indexedDocumentIndex, indexedDocumentType, indexedDocumentId
                    );
                }
                if(getResponse.isSourceEmpty()) {
                    throw new IllegalArgumentException(
                        "indexed document [" + indexedDocumentIndex + "/" + indexedDocumentType + "/" + indexedDocumentId
                            + "] source disabled"
                    );
                }
                documentSupplier.set(getResponse.getSourceAsBytesRef());
                listener.onResponse(null);
            }, listener::onFailure));
        });
        return new PercolateQueryBuilder(field, documentType, documentSupplier::get);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // Call nowInMillis() so that this query becomes un-cacheable since we
        // can't be sure that it doesn't use now or scripts
        context.nowInMillis();
        if (indexedDocumentIndex != null || indexedDocumentType != null || indexedDocumentId != null || documentSupplier != null) {
            throw new IllegalStateException("query builder must be rewritten first");
        }

        if (document == null) {
            throw new IllegalStateException("no document to percolate");
        }

        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            throw new QueryShardException(context, "field [" + field + "] does not exist");
        }

        if (!(fieldType instanceof PercolatorFieldMapper.FieldType)) {
            throw new QueryShardException(context, "expected field [" + field +
                "] to be of type [percolator], but is of type [" + fieldType.typeName() + "]");
        }

        final ParsedDocument doc;
        final DocumentMapper docMapper;
        final MapperService mapperService = context.getMapperService();
        if (context.getIndexSettings().isSingleType()) {
            Collection<String> types = mapperService.types();
            if (types.size() != 1) {
                throw new IllegalStateException("Only a single type should exist, but [" + types.size() + " types exists");
            }
            String type = types.iterator().next();
            if (documentType != null) {
                DEPRECATION_LOGGER.deprecated("[document_type] parameter has been deprecated because types have been deprecated");
                if (documentType.equals(type) == false) {
                    throw new IllegalArgumentException("specified document_type [" + documentType +
                        "] is not equal to the actual type [" + type + "]");
                }
            }
            docMapper = mapperService.documentMapper(type);
            doc = docMapper.parse(source(context.index().getName(), type, "_temp_id", document, documentXContentType));
        } else {
            if (documentType == null) {
                throw new IllegalArgumentException("[percolate] query is missing required [document_type] parameter");
            }
            DocumentMapperForType docMapperForType = mapperService.documentMapperWithAutoCreate(documentType);
            docMapper = docMapperForType.getDocumentMapper();
            doc = docMapper.parse(source(context.index().getName(), documentType, "_temp_id", document, documentXContentType));
        }

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
                    return context.getIndexAnalyzers().getDefaultIndexAnalyzer();
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

        PercolatorFieldMapper percolatorFieldMapper = (PercolatorFieldMapper) docMapper.mappers().getMapper(field);
        boolean mapUnmappedFieldsAsString = percolatorFieldMapper.isMapUnmappedFieldAsText();
        QueryShardContext percolateShardContext = wrap(context);

        PercolatorFieldMapper.FieldType pft = (PercolatorFieldMapper.FieldType) fieldType;
        PercolateQuery.QueryStore queryStore = createStore(pft.queryBuilderField, percolateShardContext, mapUnmappedFieldsAsString);
        return pft.percolateQuery(queryStore, document, docSearcher);
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

    //pkg-private for testing
    XContentType getXContentType() {
        return documentXContentType;
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

    static PercolateQuery.QueryStore createStore(MappedFieldType queryBuilderFieldType,
                                                 QueryShardContext context,
                                                 boolean mapUnmappedFieldsAsString) {
        Version indexVersion = context.indexVersionCreated();
        NamedWriteableRegistry registry = context.getWriteableRegistry();
        return ctx -> {
            LeafReader leafReader = ctx.reader();
            BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(queryBuilderFieldType.name());
            if (binaryDocValues == null) {
                return docId -> null;
            }
            if (indexVersion.onOrAfter(Version.V_6_0_0_beta2)) {
                return docId -> {
                    if (binaryDocValues.advanceExact(docId)) {
                        BytesRef qbSource = binaryDocValues.binaryValue();
                        try (InputStream in = new ByteArrayInputStream(qbSource.bytes, qbSource.offset, qbSource.length)) {
                            try (StreamInput input = new NamedWriteableAwareStreamInput(
                                    new InputStreamStreamInput(in, qbSource.length), registry)) {
                                input.setVersion(indexVersion);
                                // Query builder's content is stored via BinaryFieldMapper, which has a custom encoding
                                // to encode multiple binary values into a single binary doc values field.
                                // This is the reason we need to first need to read the number of values and
                                // then the length of the field value in bytes.
                                int numValues = input.readVInt();
                                assert numValues == 1;
                                int valueLength = input.readVInt();
                                assert valueLength > 0;
                                QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                                assert in.read() == -1;
                                return PercolatorFieldMapper.toQuery(context, mapUnmappedFieldsAsString, queryBuilder);
                            }
                        }
                    } else {
                        return null;
                    }
                };
            } else {
                return docId -> {
                    if (binaryDocValues.advanceExact(docId)) {
                        BytesRef qbSource = binaryDocValues.binaryValue();
                        if (qbSource.length > 0) {
                            XContent xContent = PercolatorFieldMapper.QUERY_BUILDER_CONTENT_TYPE.xContent();
                            try (XContentParser sourceParser = xContent.createParser(context.getXContentRegistry(), qbSource.bytes,
                                qbSource.offset, qbSource.length)) {
                                return parseQuery(context, mapUnmappedFieldsAsString, sourceParser);
                            }
                        } else {
                            return null;
                        }
                    } else {
                        return null;
                    }
                };
            }
        };
    }

    static QueryShardContext wrap(QueryShardContext shardContext) {
        return new QueryShardContext(shardContext) {

            @Override
            public BitSetProducer bitsetFilter(Query query) {
                return context -> {
                    final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
                    final IndexSearcher searcher = new IndexSearcher(topLevelContext);
                    searcher.setQueryCache(null);
                    final Weight weight = searcher.createNormalizedWeight(query, false);
                    final Scorer s = weight.scorer(context);

                    if (s != null) {
                        return new BitDocIdSet(BitSet.of(s.iterator(), context.reader().maxDoc())).bits();
                    } else {
                        return null;
                    }
                };
            }

            @Override
            @SuppressWarnings("unchecked")
            public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
                IndexFieldData.Builder builder = fieldType.fielddataBuilder();
                IndexFieldDataCache cache = new IndexFieldDataCache.None();
                CircuitBreakerService circuitBreaker = new NoneCircuitBreakerService();
                return (IFD) builder.build(shardContext.getIndexSettings(), fieldType, cache, circuitBreaker,
                        shardContext.getMapperService());
            }
        };
    }

}
