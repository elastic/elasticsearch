/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

public class ModuleScopeParser extends ScopeParser {

    public static ParseField MODULE_PARSEFIELD = new ParseField("module");

    protected String moduleScopeName;

    public ModuleScopeParser(String policyName, XContentParser policyParser) {
        super(policyName, policyParser);
    }

    @Override
    protected void setScopeName(String scopeName) {
        this.moduleScopeName = scopeName;
    }

    @Override
    protected String getScopeName() {
        return moduleScopeName;
    }
}
