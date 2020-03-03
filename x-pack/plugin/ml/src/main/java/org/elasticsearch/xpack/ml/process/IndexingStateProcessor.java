/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reads state documents of a stream, splits them and persists to an index via a bulk request.
 *
 * Some types of state, for example data frame analytics state and categorizer state, are written multiple times with the same document id.
 * The code needs to make sure that even after .ml-state index rollover there are no duplicate documents across the .ml-state*
 * indices. Such duplicates are undesirable for at least two reasons:
 *  1. We deliberately have no mappings on the state index so we cannot sort and filter in a search
 *  2. The state documents are large, so having dead documents with duplicate IDs is suboptimal from a disk usage perspective
 *
 * In order to avoid duplicates the following sequence of steps is executed every time the document is about to get persisted:
 *  1. The first non-blank line is extracted from the given bytes. Lines are delimited by the new line character ('\n')
 *  2. Document id is extracted from this line.
 *  3. Document with this id is searched for in .ml-state* indices
 *  4. If the document is found, it is overwritten in place (i.e. in the same index) with the new content.
 *     Otherwise, it is written to the index pointed by the current write alias, i.e. .ml-state-writei
 */
public class IndexingStateProcessor implements StateProcessor {

    private static final Logger LOGGER = LogManager.getLogger(IndexingStateProcessor.class);

    private static final int READ_BUF_SIZE = 8192;

    private final String jobId;
    private final AbstractAuditor<? extends AbstractAuditMessage> auditor;
    private final ResultsPersisterService resultsPersisterService;

    public IndexingStateProcessor(String jobId,
                                  ResultsPersisterService resultsPersisterService,
                                  AbstractAuditor<? extends AbstractAuditMessage> auditor) {
        this.jobId = jobId;
        this.resultsPersisterService = resultsPersisterService;
        this.auditor = auditor;
    }

    @Override
    public void process(InputStream in) throws IOException {
        BytesReference bytesToDate = null;
        List<BytesReference> newBlocks = new ArrayList<>();
        byte[] readBuf = new byte[READ_BUF_SIZE];
        int searchFrom = 0;
        // The original implementation of this loop created very deeply nested
        // CompositeBytesReference objects, which caused problems for the bulk persister.
        // This new implementation uses an intermediate List of blocks that don't contain
        // end markers to avoid such deep nesting in the CompositeBytesReference that
        // eventually gets created.
        for (int bytesRead = in.read(readBuf); bytesRead != -1; bytesRead = in.read(readBuf)) {
            BytesArray newBlock = new BytesArray(readBuf, 0, bytesRead);
            newBlocks.add(newBlock);
            if (findNextZeroByte(newBlock, 0, 0) == -1) {
                searchFrom += bytesRead;
            } else {
                BytesReference newBytes = new CompositeBytesReference(newBlocks.toArray(new BytesReference[0]));
                bytesToDate = (bytesToDate == null) ? newBytes : new CompositeBytesReference(bytesToDate, newBytes);
                bytesToDate = splitAndPersist(bytesToDate, searchFrom);
                searchFrom = (bytesToDate == null) ? 0 : bytesToDate.length();
                newBlocks.clear();
            }
            readBuf = new byte[READ_BUF_SIZE];
        }
    }

    /**
     * Splits bulk data streamed from the C++ process on '\0' characters.  The
     * data is expected to be a series of Elasticsearch bulk requests in UTF-8 JSON
     * (as would be uploaded to the public REST API) separated by zero bytes ('\0').
     */
    private BytesReference splitAndPersist(BytesReference bytesRef, int searchFrom) throws IOException {
        int splitFrom = 0;
        while (true) {
            int nextZeroByte = findNextZeroByte(bytesRef, searchFrom, splitFrom);
            if (nextZeroByte == -1) {
                // No more zero bytes in this block
                break;
            }
            // Ignore completely empty chunks
            if (nextZeroByte > splitFrom) {
                // No validation - assume the native process has formatted the state correctly
                findAppropriateIndexOrAliasAndPersist(bytesRef.slice(splitFrom, nextZeroByte - splitFrom));
            }
            splitFrom = nextZeroByte + 1;
        }
        if (splitFrom >= bytesRef.length()) {
            return null;
        }
        return bytesRef.slice(splitFrom, bytesRef.length() - splitFrom);
    }

    /**
     * Finds an appropriate index the document should be put in and then persists the document in that index.
     * For what is considered to be "appropriate" see the class documentation.
     */
    void findAppropriateIndexOrAliasAndPersist(BytesReference bytes) throws IOException {
        String firstNonBlankLine = extractFirstNonBlankLine(bytes);
        if (firstNonBlankLine == null) {
            return;
        }
        String stateDocId = extractDocId(firstNonBlankLine);
        String indexOrAlias = getConcreteIndexOrWriteAlias(stateDocId);
        persist(indexOrAlias, bytes);
    }

    void persist(String indexOrAlias, BytesReference bytes) throws IOException {
        BulkRequest bulkRequest = new BulkRequest().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkRequest.add(bytes, indexOrAlias, XContentType.JSON);
        if (bulkRequest.numberOfActions() > 0) {
            LOGGER.trace("[{}] Persisting job state document: index [{}], length [{}]", jobId, indexOrAlias, bytes.length());
            try {
                resultsPersisterService.bulkIndexWithRetry(bulkRequest,
                    jobId,
                    () -> true,
                    (msg) -> auditor.warning(jobId, "Bulk indexing of state failed " + msg));
            } catch (Exception ex) {
                String msg = "failed indexing updated state docs";
                LOGGER.error(() -> new ParameterizedMessage("[{}] {}", jobId, msg), ex);
                auditor.error(jobId, msg + " error: " + ex.getMessage());
            }
        }
    }

    private static int findNextZeroByte(BytesReference bytesRef, int searchFrom, int splitFrom) {
        return bytesRef.indexOf((byte)0, Math.max(searchFrom, splitFrom));
    }

    @SuppressWarnings("unchecked")
    /**
     * Extracts document id from the given {@code bytesRef}.
     * Only first non-blank line is parsed and document id is assumed to be a nested "index._id" field of type String.
     */
    static String extractDocId(String firstNonBlankLine) throws IOException {
        try (XContentParser parser =
                 JsonXContent.jsonXContent.createParser(
                     NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, firstNonBlankLine)) {
            Map<String, Object> map = parser.map();
            if ((map.get("index") instanceof Map) == false) {
                throw new IllegalStateException("Could not extract \"index\" field out of [" + firstNonBlankLine + "]");
            }
            map = (Map<String, Object>)map.get("index");
            if ((map.get("_id") instanceof String) == false) {
                throw new IllegalStateException("Could not extract \"index._id\" field out of [" + firstNonBlankLine + "]");
            }
            return (String)map.get("_id");
        }
    }

    /**
     * Extracts the first non-blank line from the given {@code bytesRef}.
     * Lines are separated by the new line character ('\n').
     * A line is considered blank if it only consists of space characters (' ').
     */
    private static String extractFirstNonBlankLine(BytesReference bytesRef) {
        for (int searchFrom = 0; searchFrom < bytesRef.length();) {
            int newLineMarkerIndex = bytesRef.indexOf((byte) '\n', searchFrom);
            int searchTo = newLineMarkerIndex != -1 ? newLineMarkerIndex : bytesRef.length();
            if (isBlank(bytesRef, searchFrom, searchTo) == false) {
                return bytesRef.slice(searchFrom, searchTo - searchFrom).utf8ToString();
            }
            searchFrom = newLineMarkerIndex != -1 ? newLineMarkerIndex + 1 : bytesRef.length();
        }
        return null;
    }

    /**
     * Checks whether the line pointed to by a pair of indexes: {@code from} (inclusive) and {@code to} (exclusive) is blank.
     * A line is considered blank if it only consists of space characters (' ').
     */
    private static boolean isBlank(BytesReference bytesRef, int from, int to) {
        for (int i = from; i < to; ++i) {
            if (bytesRef.get(i) != ((byte) ' ')) {
                return false;
            }
        }
        return true;
    }

    private String getConcreteIndexOrWriteAlias(String documentId) {
        Objects.requireNonNull(documentId);
        SearchRequest searchRequest =
            new SearchRequest(AnomalyDetectorsIndex.jobStateIndexPattern())
                .allowPartialSearchResults(false)
                .source(
                    new SearchSourceBuilder()
                        .size(1)
                        .trackTotalHits(false)
                        .query(new BoolQueryBuilder().filter(new IdsQueryBuilder().addIds(documentId))));
        SearchResponse searchResponse =
            resultsPersisterService.searchWithRetry(
                searchRequest,
                jobId,
                () -> true,
                (msg) -> auditor.warning(jobId, documentId + " " + msg));
        return searchResponse.getHits().getHits().length > 0
            ? searchResponse.getHits().getHits()[0].getIndex()
            : AnomalyDetectorsIndex.jobStateIndexWriteAlias();
    }
}

