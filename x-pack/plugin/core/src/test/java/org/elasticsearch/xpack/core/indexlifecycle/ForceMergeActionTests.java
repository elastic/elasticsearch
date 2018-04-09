/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ForceMergeActionTests extends AbstractSerializingTestCase<ForceMergeAction> {

    @Override
    protected ForceMergeAction doParseInstance(XContentParser parser) {
        return ForceMergeAction.parse(parser);
    }

    @Override
    protected ForceMergeAction createTestInstance() {
        return new ForceMergeAction(randomIntBetween(1, 100), randomBoolean());
    }

    @Override
    protected ForceMergeAction mutateInstance(ForceMergeAction instance) {
        int maxNumSegments = instance.getMaxNumSegments();
        boolean bestCompression = instance.isBestCompression();
        if (randomBoolean()) {
            maxNumSegments = maxNumSegments + randomIntBetween(1, 10);
        } else {
            bestCompression = !bestCompression;
        }
        return new ForceMergeAction(maxNumSegments, bestCompression);
    }

    @Override
    protected Reader<ForceMergeAction> instanceReader() {
        return ForceMergeAction::new;
    }

    public void testMissingMaxNumSegments() throws IOException {
        BytesReference emptyObject = BytesReference.bytes(JsonXContent.contentBuilder().startObject().endObject());
        XContentParser parser = XContentHelper.createParser(null, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            emptyObject, XContentType.JSON);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ForceMergeAction.parse(parser));
        assertThat(e.getMessage(), equalTo("Required [max_num_segments]"));
    }

    public void testInvalidNegativeSegmentNumber() {
        Exception r = expectThrows(IllegalArgumentException.class, () -> new ForceMergeAction(randomIntBetween(-10, 0), false));
        assertThat(r.getMessage(), equalTo("[max_num_segments] must be a positive integer"));
    }

    public void testToSteps() {
        ForceMergeAction instance = createTestInstance();
        String phase = randomAlphaOfLength(5);
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        List<Step> steps = instance.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        int segmentCountIndex = 1;
        if (instance.isBestCompression()) {
            assertEquals(3, steps.size());
            UpdateBestCompressionSettingsStep firstStep = (UpdateBestCompressionSettingsStep)  steps.get(0);
            assertThat(firstStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, UpdateBestCompressionSettingsStep.NAME)));
            segmentCountIndex = 2;
        } else {
            assertEquals(2, steps.size());
        }
        ForceMergeStep firstStep = (ForceMergeStep)  steps.get(0);
        SegmentCountStep secondStep = (SegmentCountStep) steps.get(segmentCountIndex);
        assertThat(firstStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, ForceMergeStep.NAME)));
        assertThat(firstStep.getNextStepKey(), equalTo(secondStep.getKey()));
        assertThat(secondStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, SegmentCountStep.NAME)));
        assertThat(secondStep.getNextStepKey(), equalTo(nextStepKey));
    }
}
