/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.AbstractSerializingFeatureIndexBuilderTestCase;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobConfigTests;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobStateTests;

import java.io.IOException;

public class GetDataFrameJobSingleResponseTests extends AbstractSerializingFeatureIndexBuilderTestCase<GetDataFrameJobSingleResponse> {

    @Override
    protected GetDataFrameJobSingleResponse doParseInstance(XContentParser parser) throws IOException {
        return GetDataFrameJobSingleResponse.PARSER.apply(parser, null);
    }

    @Override
    protected GetDataFrameJobSingleResponse createTestInstance() {
        return new GetDataFrameJobSingleResponse(FeatureIndexBuilderJobStateTests.randomFeatureIndexBuilderJobState(),
                FeatureIndexBuilderJobConfigTests.randomFeatureIndexBuilderJobConfig());
    }

    @Override
    protected Reader<GetDataFrameJobSingleResponse> instanceReader() {
        return GetDataFrameJobSingleResponse::new;
    }

}
