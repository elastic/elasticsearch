/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
public class PutSchedulerActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private String schedulerId;

    @Before
    public void setUpSchedulerId() {
        schedulerId = SchedulerConfigTests.randomValidSchedulerId();
    }

    @Override
    protected Request createTestInstance() {
        SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(schedulerId, randomAsciiOfLength(10));
        schedulerConfig.setIndexes(Arrays.asList(randomAsciiOfLength(10)));
        schedulerConfig.setTypes(Arrays.asList(randomAsciiOfLength(10)));
        return new Request(schedulerConfig.build());
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return Request.parseRequest(schedulerId, parser, () -> matcher);
    }

}
