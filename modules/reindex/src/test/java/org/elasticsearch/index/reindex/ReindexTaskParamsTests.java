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
package org.elasticsearch.index.reindex;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


// todo: test more then the index_groups field.
public class ReindexTaskParamsTests extends AbstractXContentTestCase<ReindexTaskParams> {

    @Override
    protected ReindexTaskParams createTestInstance() {
        return new ReindexTaskParams(randomBoolean(),
            IntStream.range(0, randomInt(20)).mapToObj(i -> setOrListOfStrings()).collect(Collectors.toList()),
            new HashMap<>());
    }

    private Collection<String> setOrListOfStrings() {
        Set<String> strings = randomUnique(() -> randomAlphaOfLength(10), randomInt(10));
        if (randomBoolean()) {
            return strings;
        } else {
            return new ArrayList<>(strings);
        }
    }

    @Override
    protected ReindexTaskParams doParseInstance(XContentParser parser) throws IOException {
        return ReindexTaskParams.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(ReindexTaskParams expectedInstance, ReindexTaskParams newInstance) {
        assertNotSame(newInstance, expectedInstance);

        List<Set<String>> expectedGroups = expectedInstance.getIndexGroups().stream().map(HashSet::new).collect(Collectors.toList());
        List<Set<String>> newGroups = expectedInstance.getIndexGroups().stream().map(HashSet::new).collect(Collectors.toList());

        assertEquals(expectedGroups, newGroups);
    }

    public void testSerialization() throws IOException {
        ReindexTaskParams before = createTestInstance();

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();

        ReindexTaskParams after = new ReindexTaskParams(in);

        assertEqualInstances(before, after);
    }
}
