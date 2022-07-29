/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetInfluencersAction.Response;
import org.elasticsearch.xpack.core.ml.job.results.Influencer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GetInfluencersActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<Influencer> hits = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            Influencer influencer = new Influencer(
                randomAlphaOfLengthBetween(1, 20),
                randomAlphaOfLengthBetween(1, 20),
                randomAlphaOfLengthBetween(1, 20),
                new Date(randomNonNegativeLong()),
                randomNonNegativeLong()
            );
            influencer.setInfluencerScore(randomDouble());
            influencer.setInitialInfluencerScore(randomDouble());
            influencer.setProbability(randomDouble());
            influencer.setInterim(randomBoolean());
            hits.add(influencer);
        }
        QueryPage<Influencer> buckets = new QueryPage<>(hits, listSize, Influencer.RESULTS_FIELD);
        return new Response(buckets);
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
