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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


// todo: test more then the index_groups field.
public class ReindexTaskStateDocTests extends AbstractXContentTestCase<ReindexTaskStateDoc> {

    @Override
    protected ReindexTaskStateDoc createTestInstance() {
        return new ReindexTaskStateDoc(new ReindexRequest().setDestIndex("test"),
            IntStream.range(0, randomInt(20)).mapToObj(i -> setOrListOfStrings()).collect(Collectors.toList()));
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
    protected ReindexTaskStateDoc doParseInstance(XContentParser parser) throws IOException {
        return ReindexTaskStateDoc.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(ReindexTaskStateDoc expectedInstance, ReindexTaskStateDoc newInstance) {
        assertNotSame(newInstance, expectedInstance);

        List<Set<String>> expectedGroups = expectedInstance.getIndexGroups().stream().map(HashSet::new).collect(Collectors.toList());
        List<Set<String>> newGroups = expectedInstance.getIndexGroups().stream().map(HashSet::new).collect(Collectors.toList());

        assertEquals(expectedGroups, newGroups);
    }
}
