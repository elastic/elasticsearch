/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.test.GraalVMThreadsFilter;

import static org.hamcrest.Matchers.is;

@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class NoSecurityManagerTests extends LuceneTestCase {

    public void testPrepopulateSecurityCaller() {
        assumeTrue("Unexpected security manager:" + System.getSecurityManager(), System.getSecurityManager() == null);
        boolean isAtLeastJava17 = Runtime.version().feature() >= 17;
        boolean isPrepopulated = Security.prepopulateSecurityCaller();
        if (isAtLeastJava17) {
            assertThat(isPrepopulated, is(true));
        } else {
            assertThat(isPrepopulated, is(false));
        }
    }
}
