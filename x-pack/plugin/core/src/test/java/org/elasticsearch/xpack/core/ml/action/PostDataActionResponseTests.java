/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCountsTests;

public class PostDataActionResponseTests extends AbstractStreamableTestCase<PostDataAction.Response> {

    @Override
    protected PostDataAction.Response createTestInstance() {
        DataCounts counts = new DataCountsTests().createTestInstance();
        return new PostDataAction.Response(counts);
    }

    @Override
    protected PostDataAction.Response createBlankInstance() {
        return new PostDataAction.Response("foo") ;
    }
}
