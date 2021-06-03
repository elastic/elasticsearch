/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

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
        private final Example e;

        NamedHolder(Example e) {
            this.e = e;
        }

        NamedHolder(StreamInput in) throws IOException {
            e = in.readNamedWriteable(Example.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeNamedWriteable(e);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            NamedHolder other = (NamedHolder) obj;
            return e.equals(other.e);
        }

        @Override
        public int hashCode() {
            return e.hashCode();
        }
    }

    private static class SneakOtherSideVersionOnWire implements Writeable {
        private final Version version;

        SneakOtherSideVersionOnWire() {
            version = Version.CURRENT;
        }

        SneakOtherSideVersionOnWire(StreamInput in) throws IOException {
            version = Version.readVersion(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            Version.writeVersion(out.getVersion(), out);
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
        Example e = new Example(randomAlphaOfLength(5));
        DelayableWriteable<Example> original = DelayableWriteable.referencing(e).asSerialized(Example::new, writableRegistry());
        assertTrue(original.isSerialized());
        roundTripTestCase(original, Example::new);
    }

    public void testRoundTripFromDelayedWithNamedWriteable() throws IOException {
        NamedHolder n = new NamedHolder(new Example(randomAlphaOfLength(5)));
        DelayableWriteable<NamedHolder> original = DelayableWriteable.referencing(n).asSerialized(NamedHolder::new, writableRegistry());
        assertTrue(original.isSerialized());
        roundTripTestCase(original, NamedHolder::new);
    }

    public void testRoundTripFromDelayedFromOldVersion() throws IOException {
        Example e = new Example(randomAlphaOfLength(5));
        DelayableWriteable<Example> original = roundTrip(DelayableWriteable.referencing(e), Example::new, randomOldVersion());
        assertTrue(original.isSerialized());
        roundTripTestCase(original, Example::new);
    }

    public void testRoundTripFromDelayedFromOldVersionWithNamedWriteable() throws IOException {
        NamedHolder n = new NamedHolder(new Example(randomAlphaOfLength(5)));
        DelayableWriteable<NamedHolder> original = roundTrip(DelayableWriteable.referencing(n), NamedHolder::new, randomOldVersion());
        assertTrue(original.isSerialized());
        roundTripTestCase(original, NamedHolder::new);
    }

    public void testSerializesWithRemoteVersion() throws IOException {
        Version remoteVersion = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        DelayableWriteable<SneakOtherSideVersionOnWire> original = DelayableWriteable.referencing(new SneakOtherSideVersionOnWire());
        assertThat(roundTrip(original, SneakOtherSideVersionOnWire::new, remoteVersion).expand().version, equalTo(remoteVersion));
    }

    public void testAsSerializedIsNoopOnSerialized() throws IOException {
        Example e = new Example(randomAlphaOfLength(5));
        DelayableWriteable<Example> d = DelayableWriteable.referencing(e).asSerialized(Example::new, writableRegistry());
        assertTrue(d.isSerialized());
        assertSame(d, d.asSerialized(Example::new, writableRegistry()));
    }

    private <T extends Writeable> void roundTripTestCase(DelayableWriteable<T> original, Writeable.Reader<T> reader) throws IOException {
        DelayableWriteable<T> roundTripped = roundTrip(original, reader, Version.CURRENT);
        assertTrue(roundTripped.isSerialized());
        assertThat(roundTripped.expand(), equalTo(original.expand()));
    }

    private <T extends Writeable> DelayableWriteable<T> roundTrip(DelayableWriteable<T> original,
            Writeable.Reader<T> reader, Version version) throws IOException {
        return copyInstance(original, writableRegistry(), (out, d) -> d.writeTo(out),
            in -> DelayableWriteable.delayed(reader, in), version);
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(singletonList(
                new NamedWriteableRegistry.Entry(Example.class, "example", Example::new)));
    }

    private static Version randomOldVersion() {
        return randomValueOtherThanMany(Version.CURRENT::before, () -> VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
    }
}
