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
import org.elasticsearch.action.bulk.ColumnType;
import org.elasticsearch.action.bulk.DocumentBatch;
import org.elasticsearch.action.bulk.FieldColumn;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Parses a {@link DocumentBatch} (binary columnar format) into a list of {@link ParsedDocument}s.
 * <p>
 * This is a separate code path from {@link DocumentParser}. Instead of parsing each document's JSON
 * individually, it resolves each field mapper once and applies it across all documents in the batch,
 * leveraging the columnar layout for efficiency.
 * <p>
 * The algorithm:
 * <ol>
 *   <li>For each document in the batch, create a {@link SourceToParse} from batch metadata</li>
 *   <li>Create a per-document {@link DocumentParserContext}</li>
 *   <li>Run metadata preParse for each document</li>
 *   <li>Iterate columns: resolve each mapper once, then apply across all documents</li>
 *   <li>Run metadata postParse for each document</li>
 *   <li>Build and return a {@link ParsedDocument} for each document</li>
 * </ol>
 */
public final class BatchDocumentParser {

    private final XContentParserConfiguration parserConfiguration;
    private final MappingParserContext mappingParserContext;

    public BatchDocumentParser(XContentParserConfiguration parserConfiguration, MappingParserContext mappingParserContext) {
        this.parserConfiguration = parserConfiguration;
        this.mappingParserContext = mappingParserContext;
    }

    /**
     * Parse all documents in a batch into a list of {@link ParsedDocument}s.
     * Errors are handled per-document: if parsing a single document fails, the exception is recorded
     * and other documents continue to be parsed. Callers should check the returned list for
     * null entries (indicating failure) and retrieve errors via the corresponding exception list.
     *
     * @param batch         the columnar document batch
     * @param mappingLookup the current mapping lookup
     * @return a result containing parsed documents and per-document exceptions
     */
    public BatchResult parseBatch(DocumentBatch batch, MappingLookup mappingLookup) {
        return parseBatchInternal(batch, mappingLookup, null);
    }

    private BatchResult parseBatchInternal(DocumentBatch batch, MappingLookup mappingLookup, List<BytesReference> originalSources) {
        final int docCount = batch.docCount();
        final MetadataFieldMapper[] metadataFieldMappers = mappingLookup.getMapping().getSortedMetadataMappers();

        // Pre-compute shared collections that are the same for all documents in the batch.
        // Batch parsing does not do dynamic mapping during per-doc iteration, so these remain unmodified.
        final Set<String> sharedCopyToFields = mappingLookup.fieldTypesLookup().getCopyToDestinationFields();
        final Map<String, List<Mapper.Builder>> sharedDynamicMappers = new HashMap<>();
        final Map<String, ObjectMapper.Builder> sharedDynamicObjectMappers = new HashMap<>();
        final Map<String, List<RuntimeField>> sharedDynamicRuntimeFields = new HashMap<>();

        // Step 1: Create per-document SourceToParse and contexts
        final SourceToParse[] sources = new SourceToParse[docCount];
        final BatchDocumentParserContext[] contexts = new BatchDocumentParserContext[docCount];
        final Exception[] exceptions = new Exception[docCount];

        for (int i = 0; i < docCount; i++) {
            try {
                BytesReference src = originalSources != null ? originalSources.get(i) : BytesArray.EMPTY;
                sources[i] = createSourceToParse(batch, i, src);
                contexts[i] = new BatchDocumentParserContext(
                    mappingLookup,
                    mappingParserContext,
                    sources[i],
                    sharedCopyToFields,
                    sharedDynamicMappers,
                    sharedDynamicObjectMappers,
                    sharedDynamicRuntimeFields
                );
            } catch (Exception e) {
                exceptions[i] = e;
            }
        }

        // Step 2: Run metadata preParse for each document
        for (int i = 0; i < docCount; i++) {
            if (exceptions[i] != null) continue;
            try {
                for (MetadataFieldMapper metadataMapper : metadataFieldMappers) {
                    metadataMapper.preParse(contexts[i]);
                }
            } catch (Exception e) {
                exceptions[i] = e;
            }
        }

        // Step 3: Iterate columns - resolve mapper once per column, apply across documents
        List<FieldColumn> columns = batch.columnList();
        for (FieldColumn column : columns) {
            String fieldPath = column.fieldPath();

            // Resolve the mapper once for this column
            Mapper mapper = resolveMapper(fieldPath, mappingLookup);
            if (mapper == null) {
                continue; // Unmapped column — skip. The pre-check in performBatchOnPrimary
                // ensures we only reach here if dynamic=false (field ignored) or
                // the mapping was just updated and the field is now mapped.
            }

            // Pre-compute parent path segments once per column to avoid per-doc string splitting
            String[] parentSegments = computeParentSegments(fieldPath);

            if (mapper instanceof FieldMapper fieldMapper) {
                // Apply this field mapper across all documents that have a value
                for (int i = 0; i < docCount; i++) {
                    if (exceptions[i] != null) continue;
                    if (column.isNull(i)) continue;

                    try {
                        parseFieldForDocument(fieldMapper, column, i, contexts[i], parentSegments);
                    } catch (Exception e) {
                        exceptions[i] = e;
                    }
                }
            } else if (mapper instanceof ObjectMapper) {
                // Object columns should be encoded as BINARY containing the full object XContent.
                // Delegate to standard parsing via a binary XContent parser.
                if (column.columnType() == ColumnType.BINARY) {
                    for (int i = 0; i < docCount; i++) {
                        if (exceptions[i] != null) continue;
                        if (column.isNull(i)) continue;

                        try {
                            parseBinaryObjectForDocument(column, i, contexts[i], mapper, parentSegments);
                        } catch (Exception e) {
                            exceptions[i] = e;
                        }
                    }
                }
                // Non-binary object columns are not supported in this initial implementation
            }
            // FieldAliasMapper is skipped - aliases should not appear as batch columns
        }

        // Step 4: Run metadata postParse for each document
        for (int i = 0; i < docCount; i++) {
            if (exceptions[i] != null) continue;
            try {
                for (MetadataFieldMapper metadataMapper : metadataFieldMappers) {
                    metadataMapper.postParse(contexts[i]);
                }
            } catch (Exception e) {
                exceptions[i] = e;
            }
        }

        // Step 5: Build ParsedDocuments
        final ParsedDocument[] results = new ParsedDocument[docCount];
        for (int i = 0; i < docCount; i++) {
            if (exceptions[i] != null) continue;
            try {
                BatchDocumentParserContext ctx = contexts[i];
                CompressedXContent dynamicUpdate = DocumentParser.createDynamicUpdate(ctx);

                results[i] = new ParsedDocument(
                    ctx.version(),
                    ctx.seqID(),
                    ctx.id(),
                    ctx.routing(),
                    ctx.reorderParentAndGetDocs(),
                    new BytesArray("{\"marker\":true}"),
                    sources[i].getXContentType(),
                    dynamicUpdate,
                    XContentMeteringParserDecorator.UNKNOWN_SIZE
                ) {
                    @Override
                    public String documentDescription() {
                        IdFieldMapper idMapper = (IdFieldMapper) mappingLookup.getMapping().getMetadataMapperByName(IdFieldMapper.NAME);
                        return idMapper.documentDescription(this);
                    }
                };
            } catch (Exception e) {
                exceptions[i] = e;
            }
        }

        return new BatchResult(results, exceptions);
    }

    private static SourceToParse createSourceToParse(DocumentBatch batch, int docIndex, BytesReference source) {
        String id = batch.docId(docIndex);
        String routing = batch.docRouting(docIndex);
        XContentType xContentType = batch.docXContentType(docIndex);
        BytesRef tsid = batch.docTsid(docIndex);

        return new SourceToParse(id, source, xContentType, routing, Map.of(), tsid);
    }

    /**
     * Resolves a mapper for the given field path. Handles dot-notation paths by looking up
     * the full path directly in the mapping lookup (which stores leaf mappers by full path).
     */
    public static Mapper resolveMapper(String fieldPath, MappingLookup mappingLookup) {
        // First try direct lookup - MappingLookup stores field mappers by full dotted path
        Mapper mapper = mappingLookup.getMapper(fieldPath);
        if (mapper != null) {
            return mapper;
        }
        // Also check object mappers for object-typed columns
        ObjectMapper objectMapper = mappingLookup.objectMappers().get(fieldPath);
        if (objectMapper != null) {
            return objectMapper;
        }
        return null;
    }

    /**
     * Parses a single field value for a single document using the columnar data.
     * Creates a {@link ColumnValueXContentParser} and invokes the field mapper's parse method.
     */
    private static void parseFieldForDocument(
        FieldMapper fieldMapper,
        FieldColumn column,
        int docIndex,
        BatchDocumentParserContext context,
        String[] parentSegments
    ) throws IOException {
        // Set up the content path for this field using pre-computed parent segments
        setPathForField(parentSegments, context.path());

        try {
            XContentParser fieldParser;
            if (column.columnType() == ColumnType.BINARY) {
                // Binary columns contain raw XContent (arrays, nested objects).
                // Delegate to a real XContent parser for the binary data.
                fieldParser = ColumnValueXContentParser.forBinary(column, docIndex, context.sourceToParse().getXContentType());
            } else {
                // Leaf scalar value - use the column value parser
                fieldParser = ColumnValueXContentParser.forLeafValue(column, docIndex);
            }

            try {
                // Advance the parser to the first token (the value token)
                fieldParser.nextToken();

                // Swap the parser directly on the batch context instead of creating a Wrapper via switchParser().
                // This avoids allocating a new Wrapper object (with 18 field copies) per field × per document.
                // Safe because FieldMapper.parse/doParseMultiFields never calls switchParser internally.
                context.setParser(fieldParser);
                fieldMapper.parse(context);
            } finally {
                fieldParser.close();
            }

            // TODO: Handle copy_to fields. For now, copy_to from batch-parsed fields is skipped.
            // To implement: check fieldMapper.copyTo().copyToFields() and call parseCopyFields.
        } finally {
            // Reset the path
            resetPath(parentSegments.length, context.path());
        }
    }

    /**
     * Parses an object stored as binary XContent for a single document.
     * This delegates to the standard document parsing flow by creating an XContent parser
     * from the binary data and parsing the object tree normally.
     */
    private static void parseBinaryObjectForDocument(
        FieldColumn column,
        int docIndex,
        BatchDocumentParserContext context,
        Mapper mapper,
        String[] parentSegments
    ) throws IOException {
        setPathForField(parentSegments, context.path());

        try {
            XContentParser binaryParser = ColumnValueXContentParser.forBinary(column, docIndex, context.sourceToParse().getXContentType());
            try {
                // Advance to the first token
                binaryParser.nextToken();

                // Object parsing uses switchParser because parseObjectOrNested may recursively wrap contexts
                DocumentParserContext subContext = context.switchParser(binaryParser);
                if (mapper instanceof ObjectMapper objectMapper) {
                    DocumentParser.parseObjectOrNested(subContext.createChildContext(objectMapper));
                }
            } finally {
                binaryParser.close();
            }
        } finally {
            resetPath(parentSegments.length, context.path());
        }
    }

    private static final String[] EMPTY_SEGMENTS = new String[0];

    /**
     * Pre-computes the parent path segments for a dotted field path.
     * For "foo.bar.baz", returns ["foo", "bar"]. For "name" (no dots), returns an empty array.
     * Called once per column, not per document.
     */
    private static String[] computeParentSegments(String fieldPath) {
        int lastDot = fieldPath.lastIndexOf('.');
        if (lastDot < 0) return EMPTY_SEGMENTS;
        return fieldPath.substring(0, lastDot).split("\\.");
    }

    /**
     * Sets the content path to the parent of the given field path using pre-computed segments.
     */
    private static void setPathForField(String[] parentSegments, ContentPath path) {
        for (String segment : parentSegments) {
            path.add(segment);
        }
    }

    /**
     * Resets the content path by removing the parent segments that were added by {@link #setPathForField}.
     */
    private static void resetPath(int segmentCount, ContentPath path) {
        for (int i = 0; i < segmentCount; i++) {
            path.remove();
        }
    }

    /**
     * Result of parsing a batch of documents.
     *
     * @param documents  array of parsed documents; null entries indicate failures
     * @param exceptions array of exceptions; null entries indicate success
     */
    public record BatchResult(ParsedDocument[] documents, Exception[] exceptions) {

        /**
         * Returns the number of documents in the batch.
         */
        public int size() {
            return documents.length;
        }

        /**
         * Returns the parsed document at the given index, or null if parsing failed.
         */
        public ParsedDocument getDocument(int index) {
            return documents[index];
        }

        /**
         * Returns the exception for the given index, or null if parsing succeeded.
         */
        public Exception getException(int index) {
            return exceptions[index];
        }

        /**
         * Returns true if parsing succeeded for the given document index.
         */
        public boolean isSuccess(int index) {
            return exceptions[index] == null;
        }

        /**
         * Returns all successfully parsed documents.
         */
        public List<ParsedDocument> successfulDocuments() {
            List<ParsedDocument> result = new ArrayList<>();
            for (ParsedDocument document : documents) {
                if (document != null) {
                    result.add(document);
                }
            }
            return result;
        }
    }

    /**
     * A DocumentParserContext implementation for batch parsing.
     * Unlike the standard RootDocumentParserContext (which is a private inner class of DocumentParser),
     * this provides a mutable parser reference so that different column parsers can be swapped in
     * for each field being parsed.
     */
    static final class BatchDocumentParserContext extends DocumentParserContext {
        private final ContentPath path = new ContentPath();
        private XContentParser parser;
        private final LuceneDocument document;
        private final List<LuceneDocument> documents = new ArrayList<>();
        private final long maxAllowedNumNestedDocs;
        private long numNestedDocs;
        private boolean docsReversed = false;

        BatchDocumentParserContext(
            MappingLookup mappingLookup,
            MappingParserContext mappingParserContext,
            SourceToParse source,
            Set<String> copyToFields,
            Map<String, List<Mapper.Builder>> dynamicMappers,
            Map<String, ObjectMapper.Builder> dynamicObjectMappers,
            Map<String, List<RuntimeField>> dynamicRuntimeFields
        ) {
            super(
                mappingLookup,
                mappingParserContext,
                source,
                mappingLookup.getMapping().getRoot(),
                ObjectMapper.Dynamic.getRootDynamic(mappingLookup),
                copyToFields,
                dynamicMappers,
                dynamicObjectMappers,
                dynamicRuntimeFields
            );
            // Create a no-op parser as default. The real parser is set per-field via setParser().
            // ColumnValueXContentParser.forNullValue() provides a minimal parser that satisfies
            // the non-null requirement for metadata preParse/postParse calls.
            this.parser = ColumnValueXContentParser.forNullValue();
            this.document = new LuceneDocument();
            this.documents.add(document);
            this.maxAllowedNumNestedDocs = mappingParserContext.getIndexSettings().getMappingNestedDocsLimit();
            this.numNestedDocs = 0L;
        }

        @Override
        public Mapper getMapper(String name) {
            Mapper mapper = getMetadataMapper(name);
            if (mapper != null) {
                return mapper;
            }
            return super.getMapper(name);
        }

        @Override
        public ContentPath path() {
            return this.path;
        }

        @Override
        public XContentParser parser() {
            return this.parser;
        }

        void setParser(XContentParser parser) {
            this.parser = parser;
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
        public Iterable<LuceneDocument> nonRootDocuments() {
            if (docsReversed) {
                throw new IllegalStateException("documents are already reversed");
            }
            return documents.subList(1, documents.size());
        }

        @Override
        public BytesRef getTsid() {
            return sourceToParse().tsid();
        }

        /**
         * Returns a copy of the provided List where parent documents appear after their children.
         */
        List<LuceneDocument> reorderParentAndGetDocs() {
            if (documents.size() > 1 && docsReversed == false) {
                docsReversed = true;
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

}
