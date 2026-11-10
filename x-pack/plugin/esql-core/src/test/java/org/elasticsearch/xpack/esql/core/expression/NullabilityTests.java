/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteArrayStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.io.IOException;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.esql.core.expression.Nullability.FALSE;
import static org.elasticsearch.xpack.esql.core.expression.Nullability.TRUE;
import static org.elasticsearch.xpack.esql.core.expression.Nullability.UNKNOWN;
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
    private Nullable MAYBE = new Nullable(EMPTY, UNKNOWN);

    public void testLogicalAndOfNullabilities() {
        assertEquals(TRUE, Expressions.nullable(asList(YES)));
        assertEquals(FALSE, Expressions.nullable(asList(NO)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(MAYBE)));

        assertEquals(UNKNOWN, Expressions.nullable(asList(MAYBE, MAYBE)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(MAYBE, YES)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(MAYBE, NO)));

        assertEquals(FALSE, Expressions.nullable(asList(NO, NO)));
        assertEquals(TRUE, Expressions.nullable(asList(NO, YES)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(NO, MAYBE)));

        assertEquals(TRUE, Expressions.nullable(asList(YES, YES)));
        assertEquals(TRUE, Expressions.nullable(asList(YES, NO)));
        assertEquals(UNKNOWN, Expressions.nullable(asList(YES, MAYBE)));
    }

    public void testSerialization_legacyFormat() throws IOException {
        TransportVersion version = TransportVersionUtils.randomVersionNotSupporting(random(), Nullability.ESQL_NULLABILITY_REMOVE_UNKNOWN);

        assertArrayEquals(new byte[] { 0 }, write(TRUE, version));
        assertEquals(TRUE, read(new byte[] { 0 }, version));

        assertArrayEquals(new byte[] { 1 }, write(FALSE, version));
        assertEquals(FALSE, read(new byte[] { 1 }, version));

        assertArrayEquals(new byte[] { 2 }, write(UNKNOWN, version));
        assertEquals(UNKNOWN, read(new byte[] { 2 }, version));
    }

    public void testSerialization_modernFormat() throws IOException {
        TransportVersion version = randomFrom(Nullability.ESQL_NULLABILITY_REMOVE_UNKNOWN, TransportVersion.current());

        assertArrayEquals(new byte[] { 1 }, write(TRUE, version));
        assertEquals(TRUE, read(new byte[] { 1 }, version));

        assertArrayEquals(new byte[] { 0 }, write(FALSE, version));
        assertEquals(FALSE, read(new byte[] { 0 }, version));

        // UNKNOWN can't be represented, so it's sent the same way newer versions read a legacy UNKNOWN
        assertArrayEquals(new byte[] { 1 }, write(UNKNOWN, version));

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> read(new byte[] { 2 }, version));
        assertEquals("unexpected byte [0x02]", e.getMessage());
    }

    public void testSerialization_formatSwitchesAtPatchVersions() throws IOException {
        for (TransportVersion id = Nullability.ESQL_NULLABILITY_REMOVE_UNKNOWN; id != null; id = id.nextPatchVersion()) {
            logger.info("Testing serialization for version [{}]", id);
            TransportVersion before = new TransportVersion(id.id() - 1);
            assertArrayEquals(id.toString(), new byte[] { 1 }, write(TRUE, id));
            assertArrayEquals(before.toString(), new byte[] { 0 }, write(TRUE, before));
        }
    }

    private static Nullability read(byte[] bytes, TransportVersion version) throws IOException {
        try (ByteArrayStreamInput in = new ByteArrayStreamInput(bytes)) {
            in.setTransportVersion(version);
            return Nullability.readFrom(in);
        }
    }

    private static byte[] write(Nullability nullability, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            nullability.writeTo(out);
            return BytesReference.toBytes(out.bytes());
        }
    }
}
