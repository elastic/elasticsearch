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

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RankEvalRequestTests extends ESTestCase {

    @SuppressWarnings("resource")
    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new RankEvalPlugin().getNamedXContent());
    }

    public void testSerialization() throws IOException {
        RankEvalRequest original = createTestItem();
        RankEvalRequest deserialized = copy(original);
        assertNotSame(deserialized, original);
        assertEquals(deserialized.getRankEvalSpec(), original.getRankEvalSpec());
        assertArrayEquals(deserialized.indices(), original.indices());
        assertEquals(deserialized.indicesOptions(), original.indicesOptions());
    }

    private static RankEvalRequest createTestItem() throws IOException {
        int numberOfIndices = randomInt(3);
        String[] indices = new String[numberOfIndices];
        for (int i=0; i < numberOfIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        RankEvalRequest rankEvalRequest = new RankEvalRequest(RankEvalSpecTests.createTestItem(), indices);
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
                randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());
        rankEvalRequest.indicesOptions(indicesOptions);
        return rankEvalRequest;
    }

    private static RankEvalRequest copy(RankEvalRequest original) throws IOException {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();
        namedWriteables.add(new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetric.class, PrecisionAtK.NAME, PrecisionAtK::new));
        namedWriteables.add(
                new NamedWriteableRegistry.Entry(EvaluationMetric.class, DiscountedCumulativeGain.NAME, DiscountedCumulativeGain::new));
        namedWriteables.add(new NamedWriteableRegistry.Entry(EvaluationMetric.class, MeanReciprocalRank.NAME, MeanReciprocalRank::new));
        return ESTestCase.copyWriteable(original, new NamedWriteableRegistry(namedWriteables), in -> {
            RankEvalRequest req = new RankEvalRequest();
            req.readFrom(in);
            return req;
        });
    }
}
