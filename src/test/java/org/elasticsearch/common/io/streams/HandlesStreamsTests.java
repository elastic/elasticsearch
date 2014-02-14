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

package org.elasticsearch.common.io.streams;

import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.HandlesStreamInput;
import org.elasticsearch.common.io.stream.HandlesStreamOutput;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class HandlesStreamsTests extends ElasticsearchTestCase {

    @Test
    public void testSharedStringHandles() throws Exception {
        String test1 = "test1";
        String test2 = "test2";
        String test3 = "test3";
        String test4 = "test4";
        String test5 = "test5";
        String test6 = "test6";

        BytesStreamOutput bout = new BytesStreamOutput();
        HandlesStreamOutput out = new HandlesStreamOutput(bout);
        out.writeString(test1);
        out.writeString(test1);
        out.writeString(test2);
        out.writeString(test3);
        out.writeSharedString(test4);
        out.writeSharedString(test4);
        out.writeSharedString(test5);
        out.writeSharedString(test6);

        BytesStreamInput bin = new BytesStreamInput(bout.bytes());
        HandlesStreamInput in = new HandlesStreamInput(bin);
        String s1 = in.readString();
        String s2 = in.readString();
        String s3 = in.readString();
        String s4 = in.readString();
        String s5 = in.readSharedString();
        String s6 = in.readSharedString();
        String s7 = in.readSharedString();
        String s8 = in.readSharedString();

        assertThat(s1, equalTo(test1));
        assertThat(s2, equalTo(test1));
        assertThat(s3, equalTo(test2));
        assertThat(s4, equalTo(test3));
        assertThat(s5, equalTo(test4));
        assertThat(s6, equalTo(test4));
        assertThat(s7, equalTo(test5));
        assertThat(s8, equalTo(test6));

        assertThat(s1, not(sameInstance(s2)));
        assertThat(s5, sameInstance(s6));
    }
}
