/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
        Influencer influencer = new Influencer(jobId, randomAsciiOfLengthBetween(1, 20), randomAsciiOfLengthBetween(1, 20),
                new Date(randomPositiveLong()), randomPositiveLong(), randomIntBetween(1, 1000));
        influencer.setInterim(randomBoolean());
        influencer.setAnomalyScore(randomDouble());
        influencer.setInitialAnomalyScore(randomDouble());
        influencer.setProbability(randomDouble());
        return influencer;
    }
    @Override
    protected Influencer createTestInstance() {
        return createTestInstance(randomAsciiOfLengthBetween(1, 20));
    }

    @Override
    protected Reader<Influencer> instanceReader() {
        return Influencer::new;
    }

    @Override
    protected Influencer parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return Influencer.PARSER.apply(parser, () -> matcher);
    }

}
