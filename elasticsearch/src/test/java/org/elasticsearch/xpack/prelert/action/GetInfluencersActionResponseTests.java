/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<Influencer> hits = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            Influencer influencer = new Influencer(randomAsciiOfLengthBetween(1, 20), randomAsciiOfLengthBetween(1, 20),
                    randomAsciiOfLengthBetween(1, 20), new Date(randomPositiveLong()), randomPositiveLong(), j + 1);
            influencer.setAnomalyScore(randomDouble());
            influencer.setInitialAnomalyScore(randomDouble());
            influencer.setProbability(randomDouble());
            influencer.setInterim(randomBoolean());
            hits.add(influencer);
        }
        QueryPage<Influencer> buckets = new QueryPage<>(hits, listSize, Influencer.RESULTS_FIELD);
        return new Response(buckets);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
