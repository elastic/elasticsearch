/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.bootstrap;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.jdk.JavaVersion;

import static org.hamcrest.Matchers.is;

public class NoSecurityManagerTests extends LuceneTestCase {

    public void testPrepopulateSecurityCaller() {
        assumeTrue("Unexpected security manager:" + System.getSecurityManager(), System.getSecurityManager() == null);
        boolean isAtLeastJava17 = JavaVersion.current().compareTo(JavaVersion.parse("17")) >= 0;
        boolean isPrepopulated = Security.prepopulateSecurityCaller();
        if (isAtLeastJava17) {
            assertThat(isPrepopulated, is(true));
        } else {
            assertThat(isPrepopulated, is(false));
        }
    }
}
