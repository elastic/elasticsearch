/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TransformProgressTests extends AbstractSerializingTransformTestCase<TransformProgress> {

    public static TransformProgress randomTransformProgress() {
        return new TransformProgress(
            randomBoolean() ? null : randomLongBetween(0, 10000),
            randomBoolean() ? null : randomLongBetween(0, 10000),
            randomBoolean() ? null : randomLongBetween(1, 10000));
    }

    @Override
    protected TransformProgress doParseInstance(XContentParser parser) throws IOException {
        return TransformProgress.PARSER.apply(parser, null);
    }

    @Override
    protected TransformProgress createTestInstance() {
        return randomTransformProgress();
    }

    @Override
    protected Reader<TransformProgress> instanceReader() {
        return TransformProgress::new;
    }

    public void testPercentComplete() {
        TransformProgress progress = new TransformProgress(0L, 100L, null);
        assertThat(progress.getPercentComplete(), equalTo(100.0));

        progress = new TransformProgress(100L, 0L, null);
        assertThat(progress.getPercentComplete(), equalTo(0.0));

        progress = new TransformProgress(100L, 10000L, null);
        assertThat(progress.getPercentComplete(), equalTo(100.0));

        progress = new TransformProgress(100L, null, null);
        assertThat(progress.getPercentComplete(), equalTo(0.0));

        progress = new TransformProgress(100L, 50L, null);
        assertThat(progress.getPercentComplete(), closeTo(50.0, 0.000001));

        progress = new TransformProgress(null, 50L, 10L);
        assertThat(progress.getPercentComplete(), is(nullValue()));
    }

    public void testConstructor() {
        IllegalArgumentException ex =
            expectThrows(IllegalArgumentException.class, () -> new TransformProgress(-1L, null, null));
        assertThat(ex.getMessage(), equalTo("[total_docs] must be >0."));

        ex = expectThrows(IllegalArgumentException.class, () -> new TransformProgress(1L, -1L, null));
        assertThat(ex.getMessage(), equalTo("[docs_processed] must be >0."));

        ex = expectThrows(IllegalArgumentException.class, () -> new TransformProgress(1L, 1L, -1L));
        assertThat(ex.getMessage(), equalTo("[docs_indexed] must be >0."));
    }

    public void testBackwardsSerialization() throws IOException {
        long totalDocs = 10_000;
        long processedDocs = randomLongBetween(0, totalDocs);
        // documentsIndexed are not in past versions, so it would be zero coming in
        TransformProgress progress = new TransformProgress(totalDocs, processedDocs, 0L);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_2_0);
            progress.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_7_2_0);
                TransformProgress streamedProgress = new TransformProgress(in);
                assertEquals(progress, streamedProgress);
            }
        }

        progress = new TransformProgress(null, processedDocs, 0L);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_2_0);
            progress.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_7_2_0);
                TransformProgress streamedProgress = new TransformProgress(in);
                assertEquals(new TransformProgress(0L, 0L, 0L), streamedProgress);
            }
        }

    }

}
