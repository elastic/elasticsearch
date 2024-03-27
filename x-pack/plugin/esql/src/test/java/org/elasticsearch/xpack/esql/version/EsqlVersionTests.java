/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.version;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.elasticsearch.xpack.esql.version.EsqlVersion.*;

public class EsqlVersionTests extends ESTestCase {
    public void testVersionString() {
        assertThat(NIGHTLY.toString(), equalTo("nightly.\uD83D\uDE34"));
        assertThat(PARTY_POPPER.toString(), equalTo("2024.04.\uD83C\uDF89"));
    }

    public void testVersionId() {
        assertThat(NIGHTLY.id(), equalTo(Integer.MAX_VALUE));
        assertThat(PARTY_POPPER.id(), equalTo(202404));
    }
}
