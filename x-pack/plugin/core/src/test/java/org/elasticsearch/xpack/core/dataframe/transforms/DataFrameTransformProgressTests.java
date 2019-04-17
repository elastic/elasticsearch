/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.transforms;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class DataFrameTransformProgressTests extends AbstractSerializingDataFrameTestCase<DataFrameTransformProgress> {
    public static DataFrameTransformProgress randomDataFrameTransformProgress() {
        long totalDocs = randomNonNegativeLong();
        return new DataFrameTransformProgress(totalDocs, randomBoolean() ? null : randomLongBetween(0, totalDocs));
    }

    @Override
    protected DataFrameTransformProgress doParseInstance(XContentParser parser) throws IOException {
        return DataFrameTransformProgress.PARSER.apply(parser, null);
    }

    @Override
    protected DataFrameTransformProgress createTestInstance() {
        return randomDataFrameTransformProgress();
    }

    @Override
    protected Reader<DataFrameTransformProgress> instanceReader() {
        return DataFrameTransformProgress::new;
    }

    public void testPercentComplete() {
        DataFrameTransformProgress progress = new DataFrameTransformProgress(0L, 100L);
        assertThat(progress.getPercentComplete(), equalTo(100.0));

        progress = new DataFrameTransformProgress(100L, 0L);
        assertThat(progress.getPercentComplete(), equalTo(100.0));

        progress = new DataFrameTransformProgress(100L, 10000L);
        assertThat(progress.getPercentComplete(), equalTo(100.0));

        progress = new DataFrameTransformProgress(100L, null);
        assertThat(progress.getPercentComplete(), equalTo(0.0));

        progress = new DataFrameTransformProgress(100L, 50L);
        assertThat(progress.getPercentComplete(), closeTo(50.0, 0.000001));
    }

    public void testConstructor() {
        IllegalArgumentException ex =
            expectThrows(IllegalArgumentException.class, () -> new DataFrameTransformProgress(-1, null));
        assertThat(ex.getMessage(), equalTo("[total_docs] must be >0."));

        ex = expectThrows(IllegalArgumentException.class, () -> new DataFrameTransformProgress(1L, -1L));
        assertThat(ex.getMessage(), equalTo("[docs_remaining] must be >0."));
    }

}
