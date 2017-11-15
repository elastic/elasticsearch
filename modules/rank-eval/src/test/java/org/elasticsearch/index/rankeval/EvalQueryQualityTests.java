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

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.rankeval.RatedDocument.DocumentKey;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class EvalQueryQualityTests extends ESTestCase {

    private static NamedWriteableRegistry namedWritableRegistry = new NamedWriteableRegistry(new RankEvalPlugin().getNamedWriteables());

    public static EvalQueryQuality randomEvalQueryQuality() {
        List<DocumentKey> unknownDocs = new ArrayList<>();
        int numberOfUnknownDocs = randomInt(5);
        for (int i = 0; i < numberOfUnknownDocs; i++) {
            unknownDocs.add(new DocumentKey(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }
        int numberOfSearchHits = randomInt(5);
        List<RatedSearchHit> ratedHits = new ArrayList<>();
        for (int i = 0; i < numberOfSearchHits; i++) {
            ratedHits.add(RatedSearchHitTests.randomRatedSearchHit());
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(randomAlphaOfLength(10),
                randomDoubleBetween(0.0, 1.0, true));
        if (randomBoolean()) {
            if (randomBoolean()) {
                evalQueryQuality.setMetricDetails(new PrecisionAtK.Breakdown(randomIntBetween(0, 1000), randomIntBetween(0, 1000)));
            } else {
                evalQueryQuality.setMetricDetails(new MeanReciprocalRank.Breakdown(randomIntBetween(0, 1000)));
            }
        }
        evalQueryQuality.addHitsAndRatings(ratedHits);
        return evalQueryQuality;
    }

    public void testSerialization() throws IOException {
        EvalQueryQuality original = randomEvalQueryQuality();
        EvalQueryQuality deserialized = copy(original);
        assertEquals(deserialized, original);
        assertEquals(deserialized.hashCode(), original.hashCode());
        assertNotSame(deserialized, original);
    }

    private static EvalQueryQuality copy(EvalQueryQuality original) throws IOException {
        return ESTestCase.copyWriteable(original, namedWritableRegistry, EvalQueryQuality::new);
    }

    public void testEqualsAndHash() throws IOException {
        checkEqualsAndHashCode(randomEvalQueryQuality(), EvalQueryQualityTests::copy, EvalQueryQualityTests::mutateTestItem);
    }

    private static EvalQueryQuality mutateTestItem(EvalQueryQuality original) {
        String id = original.getId();
        double qualityLevel = original.getQualityLevel();
        List<RatedSearchHit> ratedHits = new ArrayList<>(original.getHitsAndRatings());
        MetricDetails metricDetails = original.getMetricDetails();
        switch (randomIntBetween(0, 3)) {
        case 0:
            id = id + "_";
            break;
        case 1:
            qualityLevel = qualityLevel + 0.1;
            break;
        case 2:
            if (metricDetails == null) {
                metricDetails = new PrecisionAtK.Breakdown(1, 5);
            } else {
                metricDetails = null;
            }
            break;
        case 3:
            ratedHits.add(RatedSearchHitTests.randomRatedSearchHit());
            break;
        default:
            throw new IllegalStateException("The test should only allow four parameters mutated");
        }
        EvalQueryQuality evalQueryQuality = new EvalQueryQuality(id, qualityLevel);
        evalQueryQuality.setMetricDetails(metricDetails);
        evalQueryQuality.addHitsAndRatings(ratedHits);
        return evalQueryQuality;
    }
}
