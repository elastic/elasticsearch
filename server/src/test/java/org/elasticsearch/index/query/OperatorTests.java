/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class OperatorTests extends ESTestCase {

    public void testValidOrdinals() {
        assertThat(Operator.OR.ordinal(), equalTo(0));
        assertThat(Operator.AND.ordinal(), equalTo(1));
    }

    public void testWriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.OR.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.AND.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.OR));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.AND));
            }
        }
    }

    public void testToBooleanClauseOccur() {
        assertThat(Operator.AND.toBooleanClauseOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(Operator.OR.toBooleanClauseOccur(), equalTo(BooleanClause.Occur.SHOULD));
    }

    public void testToQueryParserOperator() {
        assertThat(Operator.AND.toQueryParserOperator(), equalTo(QueryParser.Operator.AND));
        assertThat(Operator.OR.toQueryParserOperator(), equalTo(QueryParser.Operator.OR));
    }

    public void testFromString() {
        assertThat(Operator.fromString("and"), equalTo(Operator.AND));
        assertThat(Operator.fromString("AND"), equalTo(Operator.AND));
        assertThat(Operator.fromString("AnD"), equalTo(Operator.AND));
        assertThat(Operator.fromString("or"), equalTo(Operator.OR));
        assertThat(Operator.fromString("OR"), equalTo(Operator.OR));
        assertThat(Operator.fromString("Or"), equalTo(Operator.OR));
    }
}
