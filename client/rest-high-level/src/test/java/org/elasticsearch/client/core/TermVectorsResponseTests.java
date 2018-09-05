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

package org.elasticsearch.client.core;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.util.function.Predicate;


public class TermVectorsResponseTests extends AbstractXContentTestCase<TermVectorsResponse> {

    @Override
    protected TermVectorsResponse doParseInstance(XContentParser parser) throws IOException {
        return TermVectorsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected TermVectorsResponse createTestInstance() {
        String index = randomAlphaOfLength(5);
        String type = randomAlphaOfLength(5);
        String id = String.valueOf(randomIntBetween(1,100));
        long version = randomNonNegativeLong();
        long tookInMillis = randomNonNegativeLong();
        boolean found = randomBoolean();
        List<TermVectorsResponse.TermVector> tvList = null;
        if (found == true){
            boolean hasFieldStatistics = randomBoolean();
            boolean hasTermStatistics = randomBoolean();
            boolean hasScores = randomBoolean();
            boolean hasOffsets = randomBoolean();
            boolean hasPositions = randomBoolean();
            boolean hasPayloads = randomBoolean();
            int fieldsCount = randomIntBetween(1, 3);
            tvList = new ArrayList<>(fieldsCount);
            for (int i = 0; i < fieldsCount; i++) {
                tvList.add(randomTermVector(hasFieldStatistics, hasTermStatistics, hasScores, hasOffsets, hasPositions, hasPayloads));
            }
        }
        TermVectorsResponse tvresponse = new TermVectorsResponse(index, type, id, version, found, tookInMillis);
        tvresponse.setTermVectorsList(tvList);
        return tvresponse;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.endsWith("term_vectors") || field.endsWith("terms") || field.endsWith("tokens");
    }

    private TermVectorsResponse.TermVector randomTermVector(boolean hasFieldStatistics, boolean hasTermStatistics, boolean hasScores,
                                                            boolean hasOffsets, boolean hasPositions, boolean hasPayloads) {
        TermVectorsResponse.TermVector tv = new TermVectorsResponse.TermVector();
        tv.setFieldName("field" + randomAlphaOfLength(2));
        if (hasFieldStatistics) {
            long sumDocFreq = randomNonNegativeLong();
            int docCount = randomInt(1000);
            long sumTotalTermFreq = randomNonNegativeLong();
            TermVectorsResponse.TermVector.FieldStatistics fs =
                new TermVectorsResponse.TermVector.FieldStatistics(sumDocFreq, docCount, sumTotalTermFreq);
            tv.setFieldStatistics(fs);
        }
        int termsCount = randomIntBetween(1, 5);
        List<TermVectorsResponse.TermVector.Term> terms = new ArrayList<>(termsCount);
        for (int i = 0; i < termsCount; i++) {
            terms.add(randomTerm(hasTermStatistics, hasScores, hasOffsets, hasPositions, hasPayloads));
        }
        tv.setTerms(terms);
        return tv;
    }

    private TermVectorsResponse.TermVector.Term randomTerm(boolean hasTermStatistics, boolean hasScores,
                                                           boolean hasOffsets, boolean hasPositions, boolean hasPayloads) {
        TermVectorsResponse.TermVector.Term term = new TermVectorsResponse.TermVector.Term();
        term.setTerm("term" + randomAlphaOfLength(2));
        term.setTermFreq(randomInt(10000));
        if (hasTermStatistics) {
            term.setDocFreq(randomInt(1000));
            term.setTotalTermFreq(randomNonNegativeLong());
        }
        if (hasScores) term.setScore(randomFloat());
        if (hasOffsets || hasPositions || hasPayloads ){
            int tokensCount = randomIntBetween(1, 5);
            List<TermVectorsResponse.TermVector.Token> tokens = new ArrayList<>(tokensCount);
            for (int i = 0; i < tokensCount; i++) {
                TermVectorsResponse.TermVector.Token token = new TermVectorsResponse.TermVector.Token();
                if (hasOffsets) {
                    token.setStartOffset(randomInt(1000));
                    token.setEndOffset(randomInt(2000));
                }
                if (hasPositions) token.setPosition(randomInt(100));
                if (hasPayloads) token.setPayload("payload" + randomAlphaOfLength(2));
                tokens.add(token);
            }
            term.setTokens(tokens);
        }
        return term;
    }

}
