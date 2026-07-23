/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

/**
 * Constants related to the doc_type field in the inference index.
 */
public class InferenceIndexDocTypeField {

    /**
     * The type for endpoint configs
     */
    public static final String ENDPOINT_CONFIG_TYPE = "endpoint_config";

    /**
     * The type for region policies
     */
    public static final String REGION_POLICY_TYPE = "region_policy";

    /**
     * The doc_type field is used to differentiate between different types of documents stored in the inference index.
     *
     * The following document types are currently supported:
     * 1. {@link #ENDPOINT_CONFIG_TYPE}
     * 2. {@link #REGION_POLICY_TYPE}
     */
    public static final String DOC_TYPE_FIELD = "doc_type";

    private InferenceIndexDocTypeField() {}
}
