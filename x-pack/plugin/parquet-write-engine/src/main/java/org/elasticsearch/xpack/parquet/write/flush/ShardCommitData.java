/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.flush;

import java.io.IOException;
import java.util.Map;

/**
 * The user data of the one Lucene commit that the server needs as the metadata record of the shard. The engine keeps the state of its
 * flushes there, and never touches the keys that the server owns (the history, the translog, the sequence numbers), which stay as the
 * server wrote them: the sequence numbers of the engine are in the names of its files and in the table.
 */
public interface ShardCommitData {

    /** The merged file of a flush that has started to upload: {@code name|from|to|rows}. Present only while a flush is not finished. */
    String PENDING_UPLOAD = "parquet.pending_upload";

    /** The largest sequence number that the table lists for this shard. The local files up to it are redundant. */
    String UPLOADED_THROUGH_SEQ_NO = "parquet.uploaded_through_seq_no";

    /** The number of rows that this shard has uploaded, for the doc count of the shard. */
    String UPLOADED_ROWS = "parquet.uploaded_rows";

    /** A copy of the current user data. */
    Map<String, String> userData() throws IOException;

    /** Replaces all the user data in a new commit: the map must contain the keys of the server. */
    void commit(Map<String, String> userData) throws IOException;

    static long uploadedThroughSeqNo(Map<String, String> userData) {
        final String value = userData.get(UPLOADED_THROUGH_SEQ_NO);
        return value == null ? -1 : Long.parseLong(value);
    }

    static long uploadedRows(Map<String, String> userData) {
        final String value = userData.get(UPLOADED_ROWS);
        return value == null ? 0 : Long.parseLong(value);
    }
}
