/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.util.Set;

/**
 * Makes sure a field added to {@link Authentication} is considered for accounting.
 */
public class AuthenticationRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return Authentication.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of("authenticatingSubject", "effectiveSubject");
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        return Set.of(
            // Shared enum singleton; only the field reference is counted in the shallow size.
            "type",
            // a lazily parsed cache of the API key metadata, whose raw bytes are counted; usually still unset when the estimate is taken
            "parsedApiKeyMetadata"
        );
    }

    /**
     * The estimate leaves out state shared across requests, such as {@code Subject.version} and the static {@code InternalUser}s' role
     * descriptors, which a full object-graph walk counts, and it models fixed-size structures approximately.
     */
    @Override
    protected boolean assertsAgainstRamUsageTester() {
        return false;
    }

    public void testRamBytesUsedAccountsForAllReferenceFields() {
        super.testRamBytesUsedAccountsForAllReferenceFields();
    }
}
