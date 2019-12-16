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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditMessage;
import org.elasticsearch.xpack.core.common.notifications.AbstractAuditor;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;


/**
 * Reads state documents of a stream, splits them and persists to an index via a bulk request
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
                persist(bytesRef.slice(splitFrom, nextZeroByte - splitFrom));
            }
            splitFrom = nextZeroByte + 1;
        }
        if (splitFrom >= bytesRef.length()) {
            return null;
        }
        return bytesRef.slice(splitFrom, bytesRef.length() - splitFrom);
    }

    void persist(BytesReference bytes) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bytes, AnomalyDetectorsIndex.jobStateIndexWriteAlias(), XContentType.JSON);
        if (bulkRequest.numberOfActions() > 0) {
            LOGGER.trace("[{}] Persisting job state document", jobId);
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
        for (int i = Math.max(searchFrom, splitFrom); i < bytesRef.length(); ++i) {
            if (bytesRef.get(i) == 0) {
                return i;
            }
        }
        return -1;
    }
}

