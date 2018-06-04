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

package org.elasticsearch.action.admin.indices.validate.query;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ValidateQueryResponseTests extends AbstractStreamableXContentTestCase<ValidateQueryResponse> {

    @Override
    protected boolean assertToXContentEquivalence() {
        // We cannot have XContent equivalence for ValidateResponseTests as it holds the BroadcastResponse which
        // holds a List<DefaultShardOperationFailedException>. The DefaultShardOperationFailedException uses ElasticSearchException
        // for serializing/deserializing a Throwable and the serialized and deserialized versions do not match.
        return false;
    }

    protected static ValidateQueryResponse createRandomValidateQueryResponse() {
        int totalShards = randomIntBetween(1, 10);
        int successfulShards = randomIntBetween(0, totalShards);
        int failedShards = totalShards - successfulShards;
        boolean valid = failedShards == 0;
        List<QueryExplanation> queryExplanations = new ArrayList<>(totalShards);
        List<DefaultShardOperationFailedException> shardFailures = new ArrayList<>(failedShards);
        for (int i=0; i<successfulShards; i++) {
            QueryExplanation queryExplanation = QueryExplanationTests.createRandomQueryExplanation(true);
            queryExplanations.add(queryExplanation);
        }
        for (int i=0; i<failedShards; i++) {
            QueryExplanation queryExplanation = QueryExplanationTests.createRandomQueryExplanation(false);
            ElasticsearchException exc = new ElasticsearchException("some_error_" + randomInt());
            shardFailures.add(
                new DefaultShardOperationFailedException(
                    queryExplanation.getIndex(), queryExplanation.getShard(),
                    exc
                )
            );
            queryExplanations.add(queryExplanation);
        }
        Collections.shuffle(queryExplanations, random());
        Collections.shuffle(shardFailures, random());
        return new ValidateQueryResponse(valid, queryExplanations, totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ValidateQueryResponse doParseInstance(XContentParser parser) throws IOException {
        return ValidateQueryResponse.fromXContent(parser);
    }

    @Override
    protected ValidateQueryResponse createBlankInstance() {
        return new ValidateQueryResponse();
    }

    @Override
    protected ValidateQueryResponse createTestInstance() {
        return createRandomValidateQueryResponse();
    }
}
