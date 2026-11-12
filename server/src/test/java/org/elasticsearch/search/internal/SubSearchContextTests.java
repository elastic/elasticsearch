/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

public class SubSearchContextTests extends ESTestCase {

    public void testCopyIsReleasedWithItsSource() {
        try (TestSearchContext searchContext = new TestSearchContext((SearchExecutionContext) null)) {
            SubSearchContext source = new SubSearchContext(searchContext);
            try (SubSearchContext copy = new SubSearchContext(source)) {
                source.close();
                assertTrue(copy.isClosed());
                assertFalse(copy.fetchResult().hasReferences());
            }
        }
    }

    public void testCopyThatIsNotReleasedWithItsSourceIsClosedByTheCaller() {
        try (TestSearchContext searchContext = new TestSearchContext((SearchExecutionContext) null)) {
            SubSearchContext source = new SubSearchContext(searchContext);
            try (SubSearchContext copy = new SubSearchContext(source, false)) {
                source.close();
                assertFalse(copy.isClosed());
                assertTrue(copy.fetchResult().hasReferences());
                // The try-with-resources closes the copy again, which is a no-op.
                copy.close();
                assertFalse(copy.fetchResult().hasReferences());
            }
        }
    }
}
