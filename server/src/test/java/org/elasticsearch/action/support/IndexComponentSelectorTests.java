/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class IndexComponentSelectorTests extends ESTestCase {

    public void testIndexComponentSelectorFromKey() {
        assertThat(IndexComponentSelector.getByKey("data"), equalTo(IndexComponentSelector.DATA));
        assertThat(IndexComponentSelector.getByKey("failures"), equalTo(IndexComponentSelector.FAILURES));
        assertThat(IndexComponentSelector.getByKey("*"), equalTo(IndexComponentSelector.ALL_APPLICABLE));
        assertThat(IndexComponentSelector.getByKey("d*ta"), nullValue());
        assertThat(IndexComponentSelector.getByKey("_all"), nullValue());
        assertThat(IndexComponentSelector.getByKey("**"), nullValue());
        assertThat(IndexComponentSelector.getByKey("failure"), nullValue());
    }

    public void testIndexComponentSelectorFromId() {
        assertThat(IndexComponentSelector.getById((byte) 0), equalTo(IndexComponentSelector.DATA));
        assertThat(IndexComponentSelector.getById((byte) 1), equalTo(IndexComponentSelector.FAILURES));
        assertThat(IndexComponentSelector.getById((byte) 2), equalTo(IndexComponentSelector.ALL_APPLICABLE));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> IndexComponentSelector.getById((byte) 3));
        assertThat(
            exception.getMessage(),
            containsString("Unknown id of index component selector [3], available options are: {0=DATA, 1=FAILURES, 2=ALL_APPLICABLE}")
        );
    }

}
