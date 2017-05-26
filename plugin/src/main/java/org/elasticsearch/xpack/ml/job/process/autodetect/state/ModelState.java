/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.state;


/**
 * The model state does not need to be understood on the Java side.
 * The Java code only needs to know how to form the document IDs so that
 * it can retrieve and delete the correct documents.
 */
public class ModelState {

    /**
     * Legacy type, now used only as a discriminant in the document ID
     */
    public static final String TYPE = "model_state";

    public static final String documentId(String jobId, String snapshotId, int docNum) {
        return jobId + "_" + TYPE + "_" + snapshotId + "#" + docNum;
    }

    /**
     * This is how the IDs were formed in v5.4
     */
    public static final String v54DocumentId(String jobId, String snapshotId, int docNum) {
        return jobId + "-" + snapshotId + "#" + docNum;
    }

    private ModelState() {
    }
}

