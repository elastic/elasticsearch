/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCountsTests;

public class PostDataActionResponseTests extends AbstractWireSerializingTestCase<PostDataAction.Response> {

    @Override
    protected PostDataAction.Response createTestInstance() {
        DataCounts counts = new DataCountsTests().createTestInstance();
        return new PostDataAction.Response(counts);
    }

    @Override
    protected PostDataAction.Response mutateInstance(PostDataAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<PostDataAction.Response> instanceReader() {
        return PostDataAction.Response::new;
    }
}
