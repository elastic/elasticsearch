/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.percolator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataCache;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class PercolateQueryBuilder extends AbstractQueryBuilder<PercolateQueryBuilder> {
    public static final String NAME = "percolate";

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(ParseField.class);
    static final String DOCUMENT_TYPE_DEPRECATION_MESSAGE = "[types removal] Types are deprecated in [percolate] queries. "
        + "The [document_type] should no longer be specified.";
    static final String TYPE_DEPRECATION_MESSAGE = "[types removal] Types are deprecated in [percolate] queries. "
        + "The [type] of the indexed document should no longer be specified.";

    static final ParseField DOCUMENT_FIELD = new ParseField("document");
    static final ParseField DOCUMENTS_FIELD = new ParseField("documents");
    private static final ParseField NAME_FIELD = new ParseField("name");
    private static final ParseField QUERY_FIELD = new ParseField("field");
    private static final ParseField DOCUMENT_TYPE_FIELD = new ParseField("document_type");
    private static final ParseField INDEXED_DOCUMENT_FIELD_INDEX = new ParseField("index");
    private static final ParseField INDEXED_DOCUMENT_FIELD_TYPE = new ParseField("type");
    private static final ParseField INDEXED_DOCUMENT_FIELD_ID = new ParseField("id");
    private static final ParseField INDEXED_DOCUMENT_FIELD_ROUTING = new ParseField("routing");
    private static final ParseField INDEXED_DOCUMENT_FIELD_PREFERENCE = new ParseField("preference");
    private static final ParseField INDEXED_DOCUMENT_FIELD_VERSION = new ParseField("version");

    private final String field;
    private String name;
    @Deprecated
    private final String documentType;
    private final List<BytesReference> documents;
    private final XContentType documentXContentType;

    private final String indexedDocumentIndex;
    @Deprecated
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
        this(field, documentType, Collections.singletonList(document), XContentHelper.xContentType(document));
    }

    /**
     * Creates a percolator query builder instance for percolating a provided document.
     *
     * @param field                     The field that contains the percolator query
     * @param document                  The binary blob containing document to percolate
     * @param documentXContentType      The content type of the binary blob containing the document to percolate
     */
    public PercolateQueryBuilder(String field, BytesReference document, XContentType documentXContentType) {
        this(field, null, Collections.singletonList(document), documentXContentType);
    }

    /**
     * Creates a percolator query builder instance for percolating a provided document.
     *
     * @param field                     The field that contains the percolator query
     * @param documents                  The binary blob containing document to percolate
     * @param documentXContentType      The content type of the binary blob containing the document to percolate
     */
    public PercolateQueryBuilder(String field, List<BytesReference> documents, XContentType documentXContentType) {
        this(field, null, documents, documentXContentType);
    }

    @Deprecated
    public PercolateQueryBuilder(String field, String documentType, List<BytesReference> documents, XContentType documentXContentType) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
        if (documents == null) {
            throw new IllegalArgumentException("[document] is a required argument");
        }
        this.field = field;
        this.documentType = documentType;
        this.documents = documents;
        this.documentXContentType = Objects.requireNonNull(documentXContentType);
        indexedDocumentIndex = null;
        indexedDocumentType = null;
        indexedDocumentId = null;
        indexedDocumentRouting = null;
        indexedDocumentPreference = null;
        indexedDocumentVersion = null;
        this.documentSupplier = null;
    }

    protected PercolateQueryBuilder(String field, String documentType, Supplier<BytesReference> documentSupplier) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
        this.field = field;
        this.documentType = documentType;
        this.documents = Collections.emptyList();
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
    public PercolateQueryBuilder(
        String field,
        String indexedDocumentIndex,
        String indexedDocumentType,
        String indexedDocumentId,
        String indexedDocumentRouting,
        String indexedDocumentPreference,
        Long indexedDocumentVersion
    ) {
        this(
            field,
            null,
            indexedDocumentIndex,
            indexedDocumentType,
            indexedDocumentId,
            indexedDocumentRouting,
            indexedDocumentPreference,
            indexedDocumentVersion
        );
    }

    @Deprecated
    public PercolateQueryBuilder(
        String field,
        String documentType,
        String indexedDocumentIndex,
        String indexedDocumentType,
        String indexedDocumentId,
        String indexedDocumentRouting,
        String indexedDocumentPreference,
        Long indexedDocumentVersion
    ) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
        if (indexedDocumentIndex == null) {
            throw new IllegalArgumentException("[index] is a required argument");
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
        this.documents = Collections.emptyList();
        this.documentXContentType = null;
        this.documentSupplier = null;
    }

    /**
     * Read from a stream.
     */
    PercolateQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
            name = in.readOptionalString();
        }
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
        if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
            documents = in.readList(StreamInput::readBytesReference);
        } else {
            BytesReference document = in.readOptionalBytesReference();
            documents = document != null ? Collections.singletonList(document) : Collections.emptyList();
        }
        if (documents.isEmpty() == false) {
            documentXContentType = in.readEnum(XContentType.class);
        } else {
            documentXContentType = null;
        }
        documentSupplier = null;
    }

    /**
     * Sets the name used for identification purposes in <code>_percolator_document_slot</code> response field
     * when multiple percolate queries have been specified in the main query.
     */
    public PercolateQueryBuilder setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (documentSupplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(field);
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            out.writeOptionalString(name);
        }
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
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            out.writeVInt(documents.size());
            for (BytesReference document : documents) {
                out.writeBytesReference(document);
            }
        } else {
            if (documents.size() > 1) {
                throw new IllegalArgumentException("Nodes prior to 6.1.0 cannot accept multiple documents");
            }
            BytesReference doc = documents.isEmpty() ? null : documents.iterator().next();
            out.writeOptionalBytesReference(doc);
        }
        if (documents.isEmpty() == false) {
            out.writeEnum(documentXContentType);
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(DOCUMENT_TYPE_FIELD.getPreferredName(), documentType);
        builder.field(QUERY_FIELD.getPreferredName(), field);
        if (name != null) {
            builder.field(NAME_FIELD.getPreferredName(), name);
        }
        if (documents.isEmpty() == false) {
            builder.startArray(DOCUMENTS_FIELD.getPreferredName());
            for (BytesReference document : documents) {
                try (
                    XContentParser parser = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE,
                        document
                    )
                ) {
                    parser.nextToken();
                    builder.generator().copyCurrentStructure(parser);
                }
            }
            builder.endArray();
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

    private static final ConstructingObjectParser<PercolateQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        String field = (String) args[0];
        BytesReference document = (BytesReference) args[1];
        @SuppressWarnings("unchecked")
        List<BytesReference> documents = (List<BytesReference>) args[2];
        String indexedDocId = (String) args[3];
        String indexedDocIndex = (String) args[4];
        String indexDocRouting = (String) args[5];
        String indexDocPreference = (String) args[6];
        Long indexedDocVersion = (Long) args[7];
        String indexedDocType = (String) args[8];
        String docType = (String) args[9];
        if (indexedDocId != null) {
            return new PercolateQueryBuilder(
                field,
                docType,
                indexedDocIndex,
                indexedDocType,
                indexedDocId,
                indexDocRouting,
                indexDocPreference,
                indexedDocVersion
            );
        } else if (document != null) {
            return new PercolateQueryBuilder(field, docType, Collections.singletonList(document), XContentType.JSON);
        } else {
            return new PercolateQueryBuilder(field, docType, documents, XContentType.JSON);
        }
    });
    static {
        PARSER.declareString(constructorArg(), QUERY_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseDocument(p), DOCUMENT_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> parseDocument(p), DOCUMENTS_FIELD);
        PARSER.declareString(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_ID);
        PARSER.declareString(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_INDEX);
        PARSER.declareString(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_ROUTING);
        PARSER.declareString(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_PREFERENCE);
        PARSER.declareLong(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_VERSION);
        PARSER.declareStringOrNull(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_TYPE);
        PARSER.declareStringOrNull(optionalConstructorArg(), DOCUMENT_TYPE_FIELD);
        PARSER.declareString(PercolateQueryBuilder::setName, NAME_FIELD);
        PARSER.declareString(PercolateQueryBuilder::queryName, AbstractQueryBuilder.NAME_FIELD);
        PARSER.declareFloat(PercolateQueryBuilder::boost, BOOST_FIELD);
        PARSER.declareRequiredFieldSet(
            DOCUMENT_FIELD.getPreferredName(),
            DOCUMENTS_FIELD.getPreferredName(),
            INDEXED_DOCUMENT_FIELD_ID.getPreferredName()
        );
        PARSER.declareExclusiveFieldSet(
            DOCUMENT_FIELD.getPreferredName(),
            DOCUMENTS_FIELD.getPreferredName(),
            INDEXED_DOCUMENT_FIELD_ID.getPreferredName()
        );
    }

    private static BytesReference parseDocument(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            builder.flush();
            return BytesReference.bytes(builder);
        }
    }

    public static PercolateQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    protected boolean doEquals(PercolateQueryBuilder other) {
        return Objects.equals(field, other.field)
            && Objects.equals(documentType, other.documentType)
            && Objects.equals(documents, other.documents)
            && Objects.equals(indexedDocumentIndex, other.indexedDocumentIndex)
            && Objects.equals(indexedDocumentType, other.indexedDocumentType)
            && Objects.equals(documentSupplier, other.documentSupplier)
            && Objects.equals(indexedDocumentId, other.indexedDocumentId);

    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, documentType, documents, indexedDocumentIndex, indexedDocumentType, indexedDocumentId, documentSupplier);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) {
        if (documents.isEmpty() == false) {
            return this;
        } else if (documentSupplier != null) {
            final BytesReference source = documentSupplier.get();
            if (source == null) {
                return this; // not executed yet
            } else {
                PercolateQueryBuilder rewritten = new PercolateQueryBuilder(
                    field,
                    documentType,
                    Collections.singletonList(source),
                    XContentHelper.xContentType(source)
                );
                if (name != null) {
                    rewritten.setName(name);
                }
                return rewritten;
            }
        }
        GetRequest getRequest;
        if (indexedDocumentType != null) {
            deprecationLogger.critical(DeprecationCategory.TYPES, "percolate_with_type", TYPE_DEPRECATION_MESSAGE);
            getRequest = new GetRequest(indexedDocumentIndex, indexedDocumentType, indexedDocumentId);
        } else {
            getRequest = new GetRequest(indexedDocumentIndex, indexedDocumentId);
        }
        getRequest.preference("_local");
        getRequest.routing(indexedDocumentRouting);
        getRequest.preference(indexedDocumentPreference);
        if (indexedDocumentVersion != null) {
            getRequest.version(indexedDocumentVersion);
        }
        SetOnce<BytesReference> docSupplier = new SetOnce<>();
        queryRewriteContext.registerAsyncAction((client, listener) -> {
            client.get(getRequest, ActionListener.wrap(getResponse -> {
                if (getResponse.isExists() == false) {
                    throw new ResourceNotFoundException(
                        "indexed document [{}{}/{}] couldn't be found",
                        indexedDocumentIndex,
                        indexedDocumentType == null ? "" : "/" + indexedDocumentType,
                        indexedDocumentId
                    );
                }
                if (getResponse.isSourceEmpty()) {
                    throw new IllegalArgumentException(
                        "indexed document ["
                            + indexedDocumentIndex
                            + (indexedDocumentType == null ? "" : "/" + indexedDocumentType)
                            + "/"
                            + indexedDocumentId
                            + "] source disabled"
                    );
                }
                docSupplier.set(getResponse.getSourceAsBytesRef());
                listener.onResponse(null);
            }, listener::onFailure));
        });

        PercolateQueryBuilder rewritten = new PercolateQueryBuilder(field, documentType, docSupplier::get);
        if (name != null) {
            rewritten.setName(name);
        }
        return rewritten;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new ElasticsearchException(
                "[percolate] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }

        // Call nowInMillis() so that this query becomes un-cacheable since we
        // can't be sure that it doesn't use now or scripts
        context.nowInMillis();
        if (indexedDocumentIndex != null || indexedDocumentId != null || documentSupplier != null) {
            throw new IllegalStateException("query builder must be rewritten first");
        }

        if (documents.isEmpty()) {
            throw new IllegalStateException("no document to percolate");
        }

        MappedFieldType fieldType = context.getFieldType(field);
        if (fieldType == null) {
            throw new QueryShardException(context, "field [" + field + "] does not exist");
        }

        if ((fieldType instanceof PercolatorFieldMapper.PercolatorFieldType) == false) {
            throw new QueryShardException(
                context,
                "expected field [" + field + "] to be of type [percolator], but is of type [" + fieldType.typeName() + "]"
            );
        }

        final List<ParsedDocument> docs = new ArrayList<>();
        String type = context.getType();
        if (documentType != null) {
            deprecationLogger.critical(DeprecationCategory.TYPES, "percolate_with_document_type", DOCUMENT_TYPE_DEPRECATION_MESSAGE);
            if (documentType.equals(type) == false) {
                throw new IllegalArgumentException(
                    "specified document_type [" + documentType + "] is not equal to the actual type [" + type + "]"
                );
            }
        }
        for (BytesReference document : documents) {
            docs.add(context.parseDocument(new SourceToParse(context.index().getName(), type, "_temp_id", document, documentXContentType)));
        }

        // We need this custom analyzer because the default index analyzer is strict and the percolator sometimes isn't when
        // 'index.percolator.map_unmapped_fields_as_string' is enabled:
        Analyzer analyzer = new DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
            @Override
            protected Analyzer getWrappedAnalyzer(String fieldName) {
                return context.getIndexAnalyzer(f -> context.getIndexAnalyzers().getDefaultIndexAnalyzer());
            }
        };
        final IndexSearcher docSearcher;
        final boolean excludeNestedDocuments;
        if (docs.size() > 1 || docs.get(0).docs().size() > 1) {
            assert docs.size() != 1 || context.hasNested();
            docSearcher = createMultiDocumentSearcher(analyzer, docs);
            excludeNestedDocuments = context.hasNested()
                && docs.stream().map(ParsedDocument::docs).mapToInt(List::size).anyMatch(size -> size > 1);
        } else {
            MemoryIndex memoryIndex = MemoryIndex.fromDocument(docs.get(0).rootDoc(), analyzer, true, false);
            docSearcher = memoryIndex.createSearcher();
            docSearcher.setQueryCache(null);
            excludeNestedDocuments = false;
        }

        PercolatorFieldMapper.PercolatorFieldType pft = (PercolatorFieldMapper.PercolatorFieldType) fieldType;
        String queryName = this.name != null ? this.name : pft.name();
        SearchExecutionContext percolateShardContext = wrap(context);
        PercolatorFieldMapper.configureContext(percolateShardContext, pft.mapUnmappedFieldsAsText);
        ;
        PercolateQuery.QueryStore queryStore = createStore(pft.queryBuilderField, percolateShardContext);

        return pft.percolateQuery(queryName, queryStore, documents, docSearcher, excludeNestedDocuments, context.indexVersionCreated());
    }

    public String getField() {
        return field;
    }

    public String getDocumentType() {
        return documentType;
    }

    public List<BytesReference> getDocuments() {
        return documents;
    }

    // pkg-private for testing
    XContentType getXContentType() {
        return documentXContentType;
    }

    public String getQueryName() {
        return name;
    }

    static IndexSearcher createMultiDocumentSearcher(Analyzer analyzer, Collection<ParsedDocument> docs) {
        Directory directory = new ByteBuffersDirectory();
        try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(analyzer))) {
            // Indexing in order here, so that the user provided order matches with the docid sequencing:
            Iterable<LuceneDocument> iterable = () -> docs.stream().map(ParsedDocument::docs).flatMap(Collection::stream).iterator();
            indexWriter.addDocuments(iterable);

            DirectoryReader directoryReader = DirectoryReader.open(indexWriter);
            assert directoryReader.leaves().size() == 1 : "Expected single leaf, but got [" + directoryReader.leaves().size() + "]";
            final IndexSearcher slowSearcher = new IndexSearcher(directoryReader);
            slowSearcher.setQueryCache(null);
            return slowSearcher;
        } catch (IOException e) {
            throw new ElasticsearchException("Failed to create index for percolator with nested document ", e);
        }
    }

    static PercolateQuery.QueryStore createStore(MappedFieldType queryBuilderFieldType, SearchExecutionContext context) {
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
                            try (
                                StreamInput input = new NamedWriteableAwareStreamInput(
                                    new InputStreamStreamInput(in, qbSource.length),
                                    registry
                                )
                            ) {
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
                                queryBuilder = Rewriteable.rewrite(queryBuilder, context);
                                return queryBuilder.toQuery(context);
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
                            try (
                                XContentParser sourceParser = xContent.createParser(
                                    context.getXContentRegistry(),
                                    LoggingDeprecationHandler.INSTANCE,
                                    qbSource.bytes,
                                    qbSource.offset,
                                    qbSource.length
                                )
                            ) {
                                QueryBuilder queryBuilder = PercolatorFieldMapper.parseQueryBuilder(
                                    sourceParser,
                                    sourceParser.getTokenLocation()
                                );
                                queryBuilder = Rewriteable.rewrite(queryBuilder, context);
                                return queryBuilder.toQuery(context);
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

    static SearchExecutionContext wrap(SearchExecutionContext delegate) {
        return new SearchExecutionContext(delegate) {

            @Override
            public IndexReader getIndexReader() {
                // The reader that matters in this context is not the reader of the shard but
                // the reader of the MemoryIndex. We just use `null` for simplicity.
                return null;
            }

            @Override
            public BitSetProducer bitsetFilter(Query query) {
                return context -> {
                    final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
                    final IndexSearcher searcher = new IndexSearcher(topLevelContext);
                    searcher.setQueryCache(null);
                    final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
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
                IndexFieldData.Builder builder = fieldType.fielddataBuilder(delegate.getFullyQualifiedIndex().getName(), delegate::lookup);
                IndexFieldDataCache cache = new IndexFieldDataCache.None();
                CircuitBreakerService circuitBreaker = new NoneCircuitBreakerService();
                return (IFD) builder.build(cache, circuitBreaker);
            }
        };
    }
}
