/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;


/**
 * The categorizer state does not need to be understood on the Java side.
 * The Java code only needs to know how to form the document IDs so that
 * it can retrieve and delete the correct documents.
 */
public class CategorizerState {

    /**
     * Legacy type, now used only as a discriminant in the document ID
     */
    public static final String TYPE = "categorizer_state";

    public static final String documentId(String jobId, int docNum) {
        return documentPrefix(jobId) + docNum;
    }

    public static final String documentPrefix(String jobId) {
        return jobId + "_" + TYPE + "#";
    }

    /**
     * This is how the IDs were formed in v5.4
     */
    public static final String v54DocumentId(String jobId, int docNum) {
        return v54DocumentPrefix(jobId) + docNum;
    }

    public static final String v54DocumentPrefix(String jobId) {
        return jobId + "#";
    }

    /**
     * Given the id of a categorizer state document it extracts the job id
     * @param docId the categorizer state document id
     * @return the job id or {@code null} if the id is not valid
     */
    public static final String extractJobId(String docId) {
        int suffixIndex = docId.lastIndexOf("_" + TYPE);
        return suffixIndex <= 0 ? null : docId.substring(0, suffixIndex);
    }

    private CategorizerState() {
    }
}

