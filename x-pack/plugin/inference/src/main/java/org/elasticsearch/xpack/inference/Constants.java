/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

public class Constants {

    /**
     * ML managed index mappings used to be updated based on the product version.
     * We can no longer rely on Version::CURRENT to dictate the latest version for the inference
     * index mappings as the class will eventually be removed with serverless. We still need to populate the index versions
     * with a value because older nodes will still look for the product version in the mappings metadata field. So we still need to put
     * a value in the field to indicate that the mappings are newer than the older node. The easiest solution is to hardcode 8.11.0,
     * because any node from 8.10.0 onwards should be using per-index mappings versions to determine whether mappings are up-to-date.
     */
    public static final String BWC_MAPPINGS_VERSION = "8.11.0";

    private Constants() {}
}
