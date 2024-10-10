/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public abstract class EntitlementBuilder {

    protected final String policyName;
    protected final String scopeName;
    protected final XContentParser policyParser;

    protected EntitlementBuilder(String policyName, String scopeName, XContentParser policyParser) {
        this.policyName = policyName;
        this.scopeName = scopeName;
        this.policyParser = policyParser;
    }

    protected abstract void parseEntitlement() throws IOException;
}
