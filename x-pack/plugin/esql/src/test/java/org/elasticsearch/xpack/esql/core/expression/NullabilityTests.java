/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.expression.Nullability.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Nullability.TRUE;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;

public class NullabilityTests extends ESTestCase {

    public static class Nullable extends LeafExpression {

        private final Nullability nullability;

        public Nullable(Source source, Nullability nullability) {
            super(source);
            this.nullability = nullability;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getWriteableName() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Nullability nullable() {
            return nullability;
        }

        @Override
        public DataType dataType() {
            return DataType.BOOLEAN;
        }

        @Override
        protected NodeInfo<? extends Expression> info() {
            return NodeInfo.create(this, Nullable::new, nullability);
        }
    }

    private Nullable YES = new Nullable(EMPTY, TRUE);
    private Nullable NO = new Nullable(EMPTY, FALSE);

    public void testLogicalAndOfNullabilities() {
        assertEquals(TRUE, Expressions.nullable(asList(YES)));
        assertEquals(FALSE, Expressions.nullable(asList(NO)));

        assertEquals(FALSE, Expressions.nullable(asList(NO, NO)));
        assertEquals(TRUE, Expressions.nullable(asList(NO, YES)));

        assertEquals(TRUE, Expressions.nullable(asList(YES, YES)));
        assertEquals(TRUE, Expressions.nullable(asList(YES, NO)));
    }

    public void testWriteRead() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Nullability.ESQL_NULLABILITY_REMOVE_UNKNOWN);
        for (TransportVersion version : List.of(oldVersion, TransportVersion.current())) {
            for (Nullability nullability : Nullability.values()) {
                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    out.setTransportVersion(version);
                    nullability.writeTo(out);
                    try (StreamInput in = out.bytes().streamInput()) {
                        in.setTransportVersion(version);
                        assertEquals(nullability, Nullability.readFrom(in));
                        assertEquals(0, in.available());
                    }
                }
            }
        }
    }

    public void testReadOldWireFormatLegacyUnknown() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Nullability.ESQL_NULLABILITY_REMOVE_UNKNOWN);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(oldVersion);
            // The old wire format for Nullability.UNKNOWN is 2, which now translates to TRUE.
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(oldVersion);
                assertEquals(TRUE, Nullability.readFrom(in));
                assertEquals(0, in.available());
            }
        }
    }

    public void testReadOldWireFormatInvalidOrdinal() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Nullability.ESQL_NULLABILITY_REMOVE_UNKNOWN);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(oldVersion);
            // The old wire format for Nullability only has ordinals 0, 1, and 2. 3 is invalid.
            out.writeVInt(3);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(oldVersion);
                IOException e = expectThrows(IOException.class, () -> Nullability.readFrom(in));
                assertEquals("Unknown ordinal [3] for Nullability", e.getMessage());
                assertEquals(0, in.available());
            }
        }
    }
}
