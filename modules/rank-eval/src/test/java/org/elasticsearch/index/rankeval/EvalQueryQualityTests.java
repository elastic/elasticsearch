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

package org.elasticsearch.index.rankeval;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class EvalQueryQualityTests extends ESTestCase {

    public static EvalQueryQuality randomEvalQueryQuality() {
        List<RatedDocumentKey> unknownDocs = new ArrayList<>();
        int numberOfUnknownDocs = randomInt(5);
        for (int i = 0; i < numberOfUnknownDocs; i++) {
            unknownDocs.add(RatedDocumentKeyTests.createRandomRatedDocumentKey());
        }
        return new EvalQueryQuality(randomAsciiOfLength(10), randomDoubleBetween(0.0, 1.0, true), unknownDocs );
    }

    private static EvalQueryQuality copy(EvalQueryQuality original) throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            original.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                return new EvalQueryQuality(in);
            }
        }
    }

    public void testSerialization() throws IOException {
        EvalQueryQuality original = randomEvalQueryQuality();
        EvalQueryQuality deserialized = copy(original);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    public void testEqualsAndHash() throws IOException {
        EvalQueryQuality testItem = randomEvalQueryQuality();
        RankEvalTestHelper.testHashCodeAndEquals(testItem, mutateTestItem(testItem),
                copy(testItem));
    }

    private static EvalQueryQuality mutateTestItem(EvalQueryQuality original) {
        String id = original.getId();
        double qualityLevel = original.getQualityLevel();
        List<RatedDocumentKey> unknownDocs = original.getUnknownDocs();
        switch (randomIntBetween(0, 2)) {
        case 0:
            id = id + "_";
            break;
        case 1:
            qualityLevel = qualityLevel + 0.1;
            break;
        case 2:
            unknownDocs = new ArrayList<>(unknownDocs);
            unknownDocs.add(RatedDocumentKeyTests.createRandomRatedDocumentKey());
            break;
        default:
            throw new IllegalStateException("The test should only allow three parameters mutated");
        }
        return new EvalQueryQuality(id, qualityLevel, unknownDocs);
    }


}
