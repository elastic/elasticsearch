/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

public class RegexRecordSplitterContractTests extends RecordSplitterContractTests {

    private static final String TERMINATOR = "---\n";

    @Override
    protected RecordSplitter newSplitter() {
        return new RegexRecordSplitter(TERMINATOR, 16);
    }

    @Override
    protected Fixtures fixtures() {
        return new Fixtures(bytes(TERMINATOR), false, false, false, true);
    }
}
