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

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.test.ESTestCase;
import org.joda.time.DateTimeZone;

public class DocValueFormatTests extends ESTestCase {

    public void testSerialization() throws Exception {
        NamedWriteableRegistry registry = new NamedWriteableRegistry();
        registry.register(DocValueFormat.class, DocValueFormat.BOOLEAN.getWriteableName(), in -> DocValueFormat.BOOLEAN);
        registry.register(DocValueFormat.class, DocValueFormat.DateTime.NAME, DocValueFormat.DateTime::new);
        registry.register(DocValueFormat.class, DocValueFormat.Decimal.NAME, DocValueFormat.Decimal::new);
        registry.register(DocValueFormat.class, DocValueFormat.GEOHASH.getWriteableName(), in -> DocValueFormat.GEOHASH);
        registry.register(DocValueFormat.class, DocValueFormat.IP.getWriteableName(), in -> DocValueFormat.IP);
        registry.register(DocValueFormat.class, DocValueFormat.RAW.getWriteableName(), in -> DocValueFormat.RAW);

        BytesStreamOutput out = new BytesStreamOutput();
        out.writeValueFormat(DocValueFormat.BOOLEAN);
        StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes()), registry);
        assertSame(DocValueFormat.BOOLEAN, in.readValueFormat());

        DocValueFormat.Decimal decimalFormat = new DocValueFormat.Decimal("###.##");
        out = new BytesStreamOutput();
        out.writeValueFormat(decimalFormat);
        in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes()), registry);
        DocValueFormat vf = in.readValueFormat();
        assertEquals(DocValueFormat.Decimal.class, vf.getClass());
        assertEquals("###.##", ((DocValueFormat.Decimal) vf).pattern);

        DocValueFormat.DateTime dateFormat = new DocValueFormat.DateTime(Joda.forPattern("epoch_second"), DateTimeZone.forOffsetHours(1));
        out = new BytesStreamOutput();
        out.writeValueFormat(dateFormat);
        in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes()), registry);
        vf = in.readValueFormat();
        assertEquals(DocValueFormat.DateTime.class, vf.getClass());
        assertEquals("epoch_second", ((DocValueFormat.DateTime) vf).formatter.format());
        assertEquals(DateTimeZone.forOffsetHours(1), ((DocValueFormat.DateTime) vf).timeZone);

        out = new BytesStreamOutput();
        out.writeValueFormat(DocValueFormat.GEOHASH);
        in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes()), registry);
        assertSame(DocValueFormat.GEOHASH, in.readValueFormat());

        out = new BytesStreamOutput();
        out.writeValueFormat(DocValueFormat.IP);
        in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes()), registry);
        assertSame(DocValueFormat.IP, in.readValueFormat());

        out = new BytesStreamOutput();
        out.writeValueFormat(DocValueFormat.RAW);
        in = new NamedWriteableAwareStreamInput(StreamInput.wrap(out.bytes()), registry);
        assertSame(DocValueFormat.RAW, in.readValueFormat());
    }

}
