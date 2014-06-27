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
package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.DefaultHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.MutualInformation;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristic;
import org.elasticsearch.search.aggregations.bucket.significant.heuristics.SignificanceHeuristicStreams;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

/**
 *
 */
@ElasticsearchIntegrationTest.SuiteScopeTest
public class SignificantTermsUnitTests extends ElasticsearchLuceneTestCase {

    // test that stream output can actually be read - does not replace bwc test
    @Test
    public void streamResponse() throws Exception {
        SignificanceHeuristicStreams.registerStream(MutualInformation.STREAM, MutualInformation.STREAM.getName());
        SignificanceHeuristicStreams.registerStream(DefaultHeuristic.STREAM, DefaultHeuristic.STREAM.getName());
        Version version = ElasticsearchIntegrationTest.randomVersion();
        InternalSignificantTerms[] sigTerms = getRandomSignificantTerms(getRandomSignificanceheuristic());

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        out.setVersion(version);

        sigTerms[0].writeTo(out);

        // read
        ByteArrayInputStream inBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput in = new InputStreamStreamInput(inBuffer);
        in.setVersion(version);

        sigTerms[1].readFrom(in);

        if (version.onOrAfter(Version.V_1_3_0)) {
            assertTrue(sigTerms[1].significanceHeuristic.equals(sigTerms[0].significanceHeuristic));
        } else {
            assertTrue(sigTerms[1].significanceHeuristic instanceof DefaultHeuristic);
        }
    }

    InternalSignificantTerms[] getRandomSignificantTerms(SignificanceHeuristic heuristic) {
        InternalSignificantTerms[] sTerms = new InternalSignificantTerms[2];
        ArrayList<InternalSignificantTerms.Bucket> buckets = new ArrayList<>();
        if (random().nextBoolean()) {
            BytesRef term = new BytesRef("123.0");
            buckets.add(new SignificantLongTerms.Bucket(10, 20, 8, 10, 123, InternalAggregations.EMPTY));
            sTerms[0] = new SignificantLongTerms(10, 20, "some_name", null, 1, 1, heuristic, buckets);
            sTerms[1] = new SignificantLongTerms();
        } else {

            BytesRef term = new BytesRef("someterm");
            buckets.add(new SignificantStringTerms.Bucket(term, 10, 20, 8, 10, InternalAggregations.EMPTY));
            sTerms[0] = new SignificantStringTerms(10, 20, "some_name", 1, 1, heuristic, buckets);
            sTerms[1] = new SignificantStringTerms();
        }
        return sTerms;
    }

    SignificanceHeuristic getRandomSignificanceheuristic() {
        if (random().nextBoolean()) {
            return new DefaultHeuristic();
        } else {
            return new MutualInformation().setIncludeNegatives(random().nextBoolean());
        }
    }
}
