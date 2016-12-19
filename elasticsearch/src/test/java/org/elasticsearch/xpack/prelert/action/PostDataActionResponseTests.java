/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.job.DataCounts;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;
import org.joda.time.DateTime;

public class PostDataActionResponseTests extends AbstractStreamableTestCase<JobDataAction.Response> {

    @Override
    protected JobDataAction.Response createTestInstance() {
        DataCounts counts = new DataCounts(randomAsciiOfLength(10), randomIntBetween(1, 1_000_000),
                randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000), randomIntBetween(1, 1_000_000),
                new DateTime(randomDateTimeZone()).toDate(), new DateTime(randomDateTimeZone()).toDate());

        return new JobDataAction.Response(counts);
    }

    @Override
    protected JobDataAction.Response createBlankInstance() {
        return new JobDataAction.Response("foo") ;
    }
}
