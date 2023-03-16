/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.dlm;

import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DataLifecycleErrorStoreTests extends ESTestCase {

    private DataLifecycleErrorStore errorStore;

    @Before
    public void setupServices() {
        errorStore = new DataLifecycleErrorStore();
    }

    public void testRecordAndRetrieveError() {
        errorStore.recordError("test", new NullPointerException("testing"));
        assertThat(errorStore.getError("test"), is(notNullValue()));
    }

    public void testRetrieveAfterClear() {
        errorStore.recordError("test", new NullPointerException("testing"));
        errorStore.clearStore();
        assertThat(errorStore.getError("test"), is(nullValue()));
    }
}
