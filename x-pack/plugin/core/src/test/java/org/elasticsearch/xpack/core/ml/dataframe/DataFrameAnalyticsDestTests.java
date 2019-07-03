/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.InvalidIndexNameException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DataFrameAnalyticsDestTests extends AbstractSerializingTestCase<DataFrameAnalyticsDest> {

    @Override
    protected DataFrameAnalyticsDest doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsDest.createParser(false).apply(parser, null);
    }

    @Override
    protected DataFrameAnalyticsDest createTestInstance() {
        return createRandom();
    }

    public static DataFrameAnalyticsDest createRandom() {
        String index = randomAlphaOfLength(10);
        String resultsField = randomBoolean() ? null : randomAlphaOfLength(10);
        return new DataFrameAnalyticsDest(index, resultsField);
    }

    @Override
    protected Writeable.Reader<DataFrameAnalyticsDest> instanceReader() {
        return DataFrameAnalyticsDest::new;
    }

    public void testValidate_GivenIndexWithFunkyChars() {
        expectThrows(InvalidIndexNameException.class, () -> new DataFrameAnalyticsDest("<script>foo", null).validate());
    }

    public void testValidate_GivenIndexWithUppercaseChars() {
        InvalidIndexNameException e = expectThrows(InvalidIndexNameException.class,
            () -> new DataFrameAnalyticsDest("Foo", null).validate());
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(e.getMessage(), equalTo("Invalid index name [Foo], dest.index must be lowercase"));
    }

    public void testValidate_GivenValidIndexName() {
        new DataFrameAnalyticsDest("foo_bar_42", null).validate();
    }
}
