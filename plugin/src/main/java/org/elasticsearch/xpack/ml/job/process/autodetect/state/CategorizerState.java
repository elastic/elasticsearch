/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.state;


/**
 * The categorizer state does not need to be loaded on the Java side.
 * However, the Java process DOES set up a mapping on the Elasticsearch
 * index to tell Elasticsearch not to analyse the categorizer state documents
 * in any way.
 */
public class CategorizerState {
    /**
     * The type of this class used when persisting the data
     */
    public static final String TYPE = "categorizer_state";

    public static final String categorizerStateDocId(String jobId, int docNum) {
        return jobId + "#" + docNum;
    }

    private CategorizerState() {
    }
}

