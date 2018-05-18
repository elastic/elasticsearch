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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AliasMetaDataTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final AliasMetaData before =
                AliasMetaData
                        .builder("alias")
                        .filter("{ \"term\": \"foo\"}")
                        .indexRouting("indexRouting")
                        .routing("routing")
                        .searchRouting("trim,tw , ltw , lw")
                        .build();

        assertThat(before.searchRoutingValues(), equalTo(Sets.newHashSet("trim", "tw ", " ltw ", " lw")));

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final AliasMetaData after = new AliasMetaData(in);

        assertThat(after, equalTo(before));
    }
}
