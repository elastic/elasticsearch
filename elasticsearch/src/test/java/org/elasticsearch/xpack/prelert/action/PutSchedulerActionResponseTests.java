/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
    protected Response createTestInstance() {
        SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(
                SchedulerConfigTests.randomValidSchedulerId(), randomAsciiOfLength(10));
        schedulerConfig.setIndexes(Arrays.asList(randomAsciiOfLength(10)));
        schedulerConfig.setTypes(Arrays.asList(randomAsciiOfLength(10)));
        return new Response(randomBoolean(), schedulerConfig.build());
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
