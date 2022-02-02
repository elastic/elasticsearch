/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.process.autodetect.state;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The model state does not need to be understood on the Java side.
 * The Java code only needs to know how to form the document IDs so that
 * it can retrieve and delete the correct documents.
 */
public final class ModelState {

    /**
     * Legacy type, now used only as a discriminant in the document ID
     */
    public static final String TYPE = "model_state";

    private static final Pattern V_5_4_DOC_ID_REGEX = Pattern.compile("(.*)-\\d{10}#\\d+");

    public static String documentId(String jobId, String snapshotId, int docNum) {
        return jobId + "_" + TYPE + "_" + snapshotId + "#" + docNum;
    }

    /**
     * Given the id of a state document it extracts the job id
     * @param docId the state document id
     * @return the job id or {@code null} if the id is not valid
     */
    public static String extractJobId(String docId) {
        int suffixIndex = docId.lastIndexOf("_" + TYPE + "_");
        return suffixIndex <= 0 ? v54ExtractJobId(docId) : docId.substring(0, suffixIndex);
    }

    /**
     * On version 5.4 the state doc ids had a different pattern.
     * The pattern started with the job id, followed by a hyphen, the snapshot id,
     * and ended with hash and an integer.
     */
    private static String v54ExtractJobId(String docId) {
        Matcher matcher = V_5_4_DOC_ID_REGEX.matcher(docId);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    private ModelState() {}
}
