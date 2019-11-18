/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.EstimateMemoryUsageAction.Response;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EstimateMemoryUsageActionResponseTests extends AbstractSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return new Response(
            randomBoolean() ? new ByteSizeValue(randomNonNegativeLong()) : null,
            randomBoolean() ? new ByteSizeValue(randomNonNegativeLong()) : null);
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response doParseInstance(XContentParser parser) {
        return Response.PARSER.apply(parser, null);
    }

    public void testConstructor_NullValues() {
        Response response = new Response(null, null);
        assertThat(response.getExpectedMemoryWithoutDisk(), nullValue());
        assertThat(response.getExpectedMemoryWithDisk(), nullValue());
    }

    public void testConstructor_SmallValues() {
        Response response = new Response(new ByteSizeValue(120, ByteSizeUnit.KB), new ByteSizeValue(30, ByteSizeUnit.KB));
        assertThat(response.getExpectedMemoryWithoutDisk(), equalTo(new ByteSizeValue(1, ByteSizeUnit.MB)));
        assertThat(response.getExpectedMemoryWithDisk(), equalTo(new ByteSizeValue(1, ByteSizeUnit.MB)));
    }

    public void testConstructor() {
        Response response = new Response(new ByteSizeValue(20, ByteSizeUnit.MB), new ByteSizeValue(10, ByteSizeUnit.MB));
        assertThat(response.getExpectedMemoryWithoutDisk(), equalTo(new ByteSizeValue(20, ByteSizeUnit.MB)));
        assertThat(response.getExpectedMemoryWithDisk(), equalTo(new ByteSizeValue(10, ByteSizeUnit.MB)));
    }
}
