/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.Objects;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class DelayableWriteableTests extends ESTestCase {
    // NOTE: we don't use AbstractWireSerializingTestCase because we don't implement equals and hashCode.
    private static class Example implements NamedWriteable {
        private final String s;

        Example(String s) {
            this.s = s;
        }

        Example(StreamInput in) throws IOException {
            s = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(s);
        }

        @Override
        public String getWriteableName() {
            return "example";
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Example other = (Example) obj;
            return s.equals(other.s);
        }

        @Override
        public int hashCode() {
            return s.hashCode();
        }
    }

    private static class NamedHolder implements Writeable {
        private final Example e1;
        private final Example e2;

        NamedHolder(Example e) {
            this.e1 = e;
            this.e2 = e;
        }

        NamedHolder(StreamInput in) throws IOException {
            e1 = ((DelayableWriteable.Deduplicator) in).deduplicate(in.readNamedWriteable(Example.class));
            e2 = ((DelayableWriteable.Deduplicator) in).deduplicate(in.readNamedWriteable(Example.class));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(e1);
            out.writeNamedWriteable(e2);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            NamedHolder other = (NamedHolder) obj;
            return e1.equals(other.e1) && e2.equals(other.e2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(e1, e2);
        }
    }

    private static class SneakOtherSideVersionOnWire implements Writeable {
        private final TransportVersion version;

        SneakOtherSideVersionOnWire() {
            version = TransportVersion.current();
        }

        SneakOtherSideVersionOnWire(StreamInput in) throws IOException {
            version = TransportVersion.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            TransportVersion.writeVersion(out.getTransportVersion(), out);
        }
    }

    public void testRoundTripFromReferencing() throws IOException {
        Example e = new Example(randomAlphaOfLength(5));
        DelayableWriteable<Example> original = DelayableWriteable.referencing(e);
        assertFalse(original.isSerialized());
        roundTripTestCase(original, Example::new);
    }

    public void testRoundTripFromReferencingWithNamedWriteable() throws IOException {
        NamedHolder n = new NamedHolder(new Example(randomAlphaOfLength(5)));
        DelayableWriteable<NamedHolder> original = DelayableWriteable.referencing(n);
        assertFalse(original.isSerialized());
        roundTripTestCase(original, NamedHolder::new);
    }

    public void testRoundTripFromDelayed() throws IOException {
        Example e = new Example(randomAlphaOfLengthBetween(100, 1000));
        DelayableWriteable<Example> original = DelayableWriteable.referencing(e).asSerialized(Example::new, writableRegistry());
        assertTrue(original.isSerialized());
        long length = DelayableWriteable.getSerializedSize(e);
        long page = PageCacheRecycler.BYTE_PAGE_SIZE;
        assertThat(original.getSerializedSize(), equalTo(((length + page - 1) / page) * page));
        roundTripTestCase(original, Example::new);
    }

    public void testRoundTripFromDelayedWithNamedWriteable() throws IOException {
        NamedHolder n = new NamedHolder(new Example(randomAlphaOfLength(5)));
        DelayableWriteable<NamedHolder> original = DelayableWriteable.referencing(n).asSerialized(NamedHolder::new, writableRegistry());
        assertTrue(original.isSerialized());
        roundTripTestCase(original, NamedHolder::new);
        NamedHolder copy = original.expand();
        // objects have been deduplicated
        assertSame(copy.e1, copy.e2);
    }

    public void testRoundTripFromDelayedFromOldVersion() throws IOException {
        Example e = new Example(randomAlphaOfLength(5));
        DelayableWriteable<Example> original = roundTrip(DelayableWriteable.referencing(e), Example::new, randomOldVersion());
        roundTripTestCase(original, Example::new);
    }

    public void testRoundTripFromDelayedFromOldVersionWithNamedWriteable() throws IOException {
        NamedHolder n = new NamedHolder(new Example(randomAlphaOfLength(5)));
        DelayableWriteable<NamedHolder> original = roundTrip(DelayableWriteable.referencing(n), NamedHolder::new, randomOldVersion());
        roundTripTestCase(original, NamedHolder::new);
    }

    public void testSerializesWithRemoteVersion() throws IOException {
        TransportVersion remoteVersion = TransportVersionUtils.randomCompatibleVersion(random());
        DelayableWriteable<SneakOtherSideVersionOnWire> original = DelayableWriteable.referencing(new SneakOtherSideVersionOnWire());
        assertThat(roundTrip(original, SneakOtherSideVersionOnWire::new, remoteVersion).expand().version, equalTo(remoteVersion));
    }

    public void testAsSerializedIsNoopOnSerialized() throws IOException {
        Example e = new Example(randomAlphaOfLength(5));
        DelayableWriteable<Example> d = DelayableWriteable.referencing(e).asSerialized(Example::new, writableRegistry());
        assertTrue(d.isSerialized());
        assertSame(d, d.asSerialized(Example::new, writableRegistry()));
    }

    public void testPageAlignedRamUsedByReferenceBytes() {
        final int page = PageCacheRecycler.BYTE_PAGE_SIZE;
        assertThat(DelayableWriteable.pageAlignedRamUsedByReferenceBytes(BytesArray.EMPTY), equalTo(0L));
        assertThat(DelayableWriteable.pageAlignedRamUsedByReferenceBytes(new BytesArray(new byte[1])), equalTo((long) page));
        assertThat(DelayableWriteable.pageAlignedRamUsedByReferenceBytes(new BytesArray(new byte[page])), equalTo((long) page));
        assertThat(DelayableWriteable.pageAlignedRamUsedByReferenceBytes(new BytesArray(new byte[page + 1])), equalTo(2L * page));

        assertThat(
            DelayableWriteable.pageAlignedRamUsedByReferenceBytes(ReleasableBytesReference.wrap(new BytesArray(new byte[1]))),
            equalTo((long) page)
        );

        BytesReference composite = CompositeBytesReference.of(
            new BytesArray(new byte[1]),
            new BytesArray(new byte[page / 2]),
            new BytesArray(new byte[page + 1])
        );
        assertThat(DelayableWriteable.pageAlignedRamUsedByReferenceBytes(composite), equalTo(4L * page));
    }

    private <T extends Writeable> void roundTripTestCase(DelayableWriteable<T> original, Writeable.Reader<T> reader) throws IOException {
        DelayableWriteable<T> roundTripped = roundTrip(original, reader, TransportVersion.current());
        assertThat(roundTripped.expand(), equalTo(original.expand()));
    }

    private <T extends Writeable> DelayableWriteable<T> roundTrip(
        DelayableWriteable<T> original,
        Writeable.Reader<T> reader,
        TransportVersion version
    ) throws IOException {
        DelayableWriteable<T> delayed = copyInstance(
            original,
            writableRegistry(),
            StreamOutput::writeWriteable,
            in -> DelayableWriteable.delayed(reader, in),
            version
        );
        assertTrue(delayed.isSerialized());

        DelayableWriteable<T> referencing = copyInstance(
            original,
            writableRegistry(),
            StreamOutput::writeWriteable,
            in -> DelayableWriteable.referencing(reader, in),
            version
        );
        assertFalse(referencing.isSerialized());

        return randomFrom(delayed, referencing);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(singletonList(new NamedWriteableRegistry.Entry(Example.class, "example", Example::new)));
    }

    private static TransportVersion randomOldVersion() {
        return TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.minimumCompatible(),
            TransportVersionUtils.getPreviousVersion(TransportVersion.current())
        );
    }
}
