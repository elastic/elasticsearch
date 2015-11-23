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

package org.elasticsearch.plugin.ingest.transport;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class TransportDataTests extends ESTestCase {

    public void testEqualsAndHashcode() throws Exception {
        String index = randomAsciiOfLengthBetween(1, 10);
        String type = randomAsciiOfLengthBetween(1, 10);
        String id = randomAsciiOfLengthBetween(1, 10);
        String fieldName = randomAsciiOfLengthBetween(1, 10);
        String fieldValue = randomAsciiOfLengthBetween(1, 10);
        TransportData transportData = new TransportData(new IngestDocument(index, type, id, Collections.singletonMap(fieldName, fieldValue)));

        boolean changed = false;
        String otherIndex;
        if (randomBoolean()) {
            otherIndex = randomAsciiOfLengthBetween(1, 10);
            changed = true;
        } else {
            otherIndex = index;
        }
        String otherType;
        if (randomBoolean()) {
            otherType = randomAsciiOfLengthBetween(1, 10);
            changed = true;
        } else {
            otherType = type;
        }
        String otherId;
        if (randomBoolean()) {
            otherId = randomAsciiOfLengthBetween(1, 10);
            changed = true;
        } else {
            otherId = id;
        }
        Map<String, Object> document;
        if (randomBoolean()) {
            document = Collections.singletonMap(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10));
            changed = true;
        } else {
            document = Collections.singletonMap(fieldName, fieldValue);
        }

        TransportData otherTransportData = new TransportData(new IngestDocument(otherIndex, otherType, otherId, document));
        if (changed) {
            assertThat(transportData, not(equalTo(otherTransportData)));
            assertThat(otherTransportData, not(equalTo(transportData)));
        } else {
            assertThat(transportData, equalTo(otherTransportData));
            assertThat(otherTransportData, equalTo(transportData));
            TransportData thirdTransportData = new TransportData(new IngestDocument(index, type, id, Collections.singletonMap(fieldName, fieldValue)));
            assertThat(thirdTransportData, equalTo(transportData));
            assertThat(transportData, equalTo(thirdTransportData));
        }
    }

    public void testSerialization() throws IOException {
        IngestDocument ingestDocument = new IngestDocument(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10),
                Collections.singletonMap(randomAsciiOfLengthBetween(1, 10), randomAsciiOfLengthBetween(1, 10)));
        TransportData transportData = new TransportData(ingestDocument);

        BytesStreamOutput out = new BytesStreamOutput();
        transportData.writeTo(out);
        StreamInput streamInput = StreamInput.wrap(out.bytes());
        TransportData otherTransportData = TransportData.readTransportDataFrom(streamInput);
        assertThat(otherTransportData, equalTo(transportData));
    }
}
