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

package org.elasticsearch.search;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;

public class DocValueFormatTests extends ESTestCase {

    public void testSerialization() throws Exception {
        List<Entry> entries = new ArrayList<>();
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.BOOLEAN.getWriteableName(), in -> DocValueFormat.BOOLEAN));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.DateTime.NAME, DocValueFormat.DateTime::new));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.Decimal.NAME, DocValueFormat.Decimal::new));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.GEOHASH.getWriteableName(), in -> DocValueFormat.GEOHASH));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.IP.getWriteableName(), in -> DocValueFormat.IP));
        entries.add(new Entry(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW));
        NamedWriteableRegistry registry = new NamedWriteableRegistry(entries);

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.BOOLEAN);
        StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.BOOLEAN, in.readNamedWriteable(DocValueFormat.class));

        DocValueFormat.Decimal decimalFormat = new DocValueFormat.Decimal("###.##");
        out = new BytesStreamOutput();
        out.writeNamedWriteable(decimalFormat);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        DocValueFormat vf = in.readNamedWriteable(DocValueFormat.class);
        assertEquals(DocValueFormat.Decimal.class, vf.getClass());
        assertEquals("###.##", ((DocValueFormat.Decimal) vf).pattern);

        DocValueFormat.DateTime dateFormat = new DocValueFormat.DateTime(Joda.forPattern("epoch_second"), DateTimeZone.forOffsetHours(1));
        out = new BytesStreamOutput();
        out.writeNamedWriteable(dateFormat);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        vf = in.readNamedWriteable(DocValueFormat.class);
        assertEquals(DocValueFormat.DateTime.class, vf.getClass());
        assertEquals("epoch_second", ((DocValueFormat.DateTime) vf).formatter.format());
        assertEquals(DateTimeZone.forOffsetHours(1), ((DocValueFormat.DateTime) vf).timeZone);

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.GEOHASH);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.GEOHASH, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.IP);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.IP, in.readNamedWriteable(DocValueFormat.class));

        out = new BytesStreamOutput();
        out.writeNamedWriteable(DocValueFormat.RAW);
        in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry);
        assertSame(DocValueFormat.RAW, in.readNamedWriteable(DocValueFormat.class));
    }

    public void testRawFormat() {
        assertEquals("0", DocValueFormat.RAW.format(0));
        assertEquals("-1", DocValueFormat.RAW.format(-1));
        assertEquals("1", DocValueFormat.RAW.format(1));

        assertEquals("0.0", DocValueFormat.RAW.format(0d));
        assertEquals("0.5", DocValueFormat.RAW.format(.5d));
        assertEquals("-1.0", DocValueFormat.RAW.format(-1d));

        assertEquals("abc", DocValueFormat.RAW.format(new BytesRef("abc")));
    }

    public void testBooleanFormat() {
        assertEquals("false", DocValueFormat.BOOLEAN.format(0));
        assertEquals("true", DocValueFormat.BOOLEAN.format(1));
    }

    public void testIpFormat() {
        assertEquals("192.168.1.7",
                DocValueFormat.IP.format(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.7")))));
        assertEquals("::1",
                DocValueFormat.IP.format(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1")))));
    }

    public void testRawParse() {
        assertEquals(-1L, DocValueFormat.RAW.parseLong("-1", randomBoolean(), null));
        assertEquals(1L, DocValueFormat.RAW.parseLong("1", randomBoolean(), null));
        // not checking exception messages as they could depend on the JVM
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseLong("", randomBoolean(), null));
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseLong("abc", randomBoolean(), null));

        assertEquals(-1d, DocValueFormat.RAW.parseDouble("-1", randomBoolean(), null), 0d);
        assertEquals(1d, DocValueFormat.RAW.parseDouble("1", randomBoolean(), null), 0d);
        assertEquals(.5, DocValueFormat.RAW.parseDouble("0.5", randomBoolean(), null), 0d);
        // not checking exception messages as they could depend on the JVM
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseLong("", randomBoolean(), null));
        expectThrows(IllegalArgumentException.class, () -> DocValueFormat.RAW.parseLong("abc", randomBoolean(), null));

        assertEquals(new BytesRef("abc"), DocValueFormat.RAW.parseBytesRef("abc"));
    }

    public void testBooleanParse() {
        assertEquals(0L, DocValueFormat.BOOLEAN.parseLong("false", randomBoolean(), null));
        assertEquals(1L, DocValueFormat.BOOLEAN.parseLong("true", randomBoolean(), null));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> DocValueFormat.BOOLEAN.parseLong("", randomBoolean(), null));
        assertEquals("Cannot parse boolean [], expected either [true] or [false]", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> DocValueFormat.BOOLEAN.parseLong("0", randomBoolean(), null));
        assertEquals("Cannot parse boolean [0], expected either [true] or [false]", e.getMessage());
        e = expectThrows(IllegalArgumentException.class,
                () -> DocValueFormat.BOOLEAN.parseLong("False", randomBoolean(), null));
        assertEquals("Cannot parse boolean [False], expected either [true] or [false]", e.getMessage());
    }

    public void testIPParse() {
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("192.168.1.7"))),
                DocValueFormat.IP.parseBytesRef("192.168.1.7"));
        assertEquals(new BytesRef(InetAddressPoint.encode(InetAddresses.forString("::1"))),
                DocValueFormat.IP.parseBytesRef("::1"));
    }
}
