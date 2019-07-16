/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ingest.AbstractProcessor;

public abstract class AbstractEnrichProcessor extends AbstractProcessor {

    protected final String policyName;

    protected AbstractEnrichProcessor(String tag, String policyName) {
        super(tag);
        this.policyName = policyName;
    }

    public String getPolicyName() {
        return policyName;
    }
}
