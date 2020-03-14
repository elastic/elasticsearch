/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class DelayableWriteableTests extends ESTestCase {
    // NOTE: we don't use AbstractWireSerializingTestCase because we don't implement equals and hashCode.
    public static class Example implements NamedWriteable {
        private final String s;

        public Example(String s) {
            this.s = s;
        }

        public Example(StreamInput in) throws IOException {
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

    public static class NamedHolder implements Writeable {
        private final Example e;

        public NamedHolder(Example e) {
            this.e = e;
        }

        public NamedHolder(StreamInput in) throws IOException {
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

    public void testRoundTrip() throws IOException {
        Example e = new Example(randomAlphaOfLength(5));
        roundTripTestCase(DelayableWriteable.referencing(e), Example::new);
    }

    public void testRoundTripWithNamedWriteable() throws IOException {
        NamedHolder n = new NamedHolder(new Example(randomAlphaOfLength(5)));
        roundTripTestCase(DelayableWriteable.referencing(n), NamedHolder::new);
    }

    private <T extends Writeable> void roundTripTestCase(DelayableWriteable<T> original, Writeable.Reader<T> reader) throws IOException {
        assertFalse(original.isDelayed());
        DelayableWriteable<T> roundTripped = roundTrip(original, reader, Version.CURRENT);
        assertTrue(roundTripped.isDelayed());
        assertThat(roundTripped.get(), equalTo(original.get()));
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
}
