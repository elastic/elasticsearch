/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.test.unit.common.io.streams;

import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.HandlesStreamInput;
import org.elasticsearch.common.io.stream.HandlesStreamOutput;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class HandlesStreamsTests {

    @Test
    public void testSharedUTFHandles() throws Exception {
        BytesStreamOutput bytesOut = new BytesStreamOutput();
        HandlesStreamOutput out = new HandlesStreamOutput(bytesOut, 5);
        String lowerThresholdValue = "test";
        String higherThresholdValue = "something that is higher than 5";
        out.writeUTF(lowerThresholdValue);
        out.writeUTF(higherThresholdValue);
        out.writeInt(1);
        out.writeUTF("else");
        out.writeUTF(higherThresholdValue);
        out.writeUTF(lowerThresholdValue);

        HandlesStreamInput in = new HandlesStreamInput(new BytesStreamInput(bytesOut.bytes().toBytes(), false));
        assertThat(in.readUTF(), equalTo(lowerThresholdValue));
        assertThat(in.readUTF(), equalTo(higherThresholdValue));
        assertThat(in.readInt(), equalTo(1));
        assertThat(in.readUTF(), equalTo("else"));
        assertThat(in.readUTF(), equalTo(higherThresholdValue));
        assertThat(in.readUTF(), equalTo(lowerThresholdValue));
    }
}
