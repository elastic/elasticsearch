/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.lucene.codecs.Codec;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ForceMergeActionTests extends AbstractActionTestCase<ForceMergeAction> {

    @Override
    protected ForceMergeAction doParseInstance(XContentParser parser) {
        return ForceMergeAction.parse(parser);
    }

    @Override
    protected ForceMergeAction createTestInstance() {
        return randomInstance();
    }

    static ForceMergeAction randomInstance() {
        return new ForceMergeAction(randomIntBetween(1, 100), createRandomCompressionSettings());
    }

    static Codec createRandomCompressionSettings() {
        if (randomBoolean()) {
            return null;
        }
        return Codec.forName(CodecService.BEST_COMPRESSION_CODEC);
    }

    @Override
    protected ForceMergeAction mutateInstance(ForceMergeAction instance) {
        int maxNumSegments = instance.getMaxNumSegments();
        maxNumSegments = maxNumSegments + randomIntBetween(1, 10);
        return new ForceMergeAction(maxNumSegments, createRandomCompressionSettings());
    }

    @Override
    protected Reader<ForceMergeAction> instanceReader() {
        return ForceMergeAction::new;
    }

    private void assertNonBestCompression(ForceMergeAction instance) {
        String phase = randomAlphaOfLength(5);
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        List<Step> steps = instance.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(3, steps.size());
        UpdateSettingsStep firstStep = (UpdateSettingsStep) steps.get(0);
        ForceMergeStep secondStep = (ForceMergeStep) steps.get(1);
        SegmentCountStep thirdStep = (SegmentCountStep) steps.get(2);
        assertThat(firstStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, ReadOnlyAction.NAME)));
        assertThat(firstStep.getNextStepKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, ForceMergeStep.NAME)));
        assertTrue(IndexMetaData.INDEX_BLOCKS_WRITE_SETTING.get(firstStep.getSettings()));
        assertThat(secondStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, ForceMergeStep.NAME)));
        assertThat(secondStep.getNextStepKey(), equalTo(thirdStep.getKey()));
        assertThat(thirdStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, SegmentCountStep.NAME)));
        assertThat(thirdStep.getNextStepKey(), equalTo(nextStepKey));
    }

    private void assertBestCompression(ForceMergeAction instance) {
        String phase = randomAlphaOfLength(5);
        StepKey nextStepKey = new StepKey(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10));
        List<Step> steps = instance.toSteps(null, phase, nextStepKey);
        assertNotNull(steps);
        assertEquals(6, steps.size());
        CloseIndexStep firstStep = (CloseIndexStep) steps.get(0);
        UpdateSettingsStep secondStep = (UpdateSettingsStep) steps.get(1);
        OpenIndexStep thirdStep = (OpenIndexStep) steps.get(2);
        WaitForIndexColorStep fourthStep = (WaitForIndexColorStep) steps.get(3);
        ForceMergeStep fifthStep = (ForceMergeStep) steps.get(4);
        SegmentCountStep sixthStep = (SegmentCountStep) steps.get(4);
        assertThat(firstStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, CloseIndexStep.NAME)));
        assertThat(firstStep.getNextStepKey(), equalTo(secondStep.getKey()));
        assertThat(secondStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, UpdateSettingsStep.NAME)));
        assertThat(secondStep.getSettings().get(EngineConfig.INDEX_CODEC_SETTING.getKey()), equalTo(CodecService.BEST_COMPRESSION_CODEC));
        assertThat(secondStep.getNextStepKey(), equalTo(thirdStep.getKey()));
        assertThat(thirdStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, OpenIndexStep.NAME)));
        assertThat(thirdStep.getNextStepKey(), equalTo(fourthStep));
        assertThat(fourthStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, WaitForIndexColorStep.NAME)));
        assertThat(fourthStep.getNextStepKey(), equalTo(fifthStep));
        assertThat(fifthStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, ForceMergeStep.NAME)));
        assertThat(fifthStep.getNextStepKey(), equalTo(nextStepKey));
        assertThat(sixthStep.getKey(), equalTo(new StepKey(phase, ForceMergeAction.NAME, SegmentCountStep.NAME)));
        assertThat(sixthStep.getNextStepKey(), equalTo(nextStepKey));
    }

    public void testMissingMaxNumSegments() throws IOException {
        BytesReference emptyObject = BytesReference.bytes(JsonXContent.contentBuilder().startObject().endObject());
        XContentParser parser = XContentHelper.createParser(null, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            emptyObject, XContentType.JSON);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ForceMergeAction.parse(parser));
        assertThat(e.getMessage(), equalTo("Required [max_num_segments]"));
    }

    public void testInvalidNegativeSegmentNumber() {
        Exception r = expectThrows(IllegalArgumentException.class, () -> new
            ForceMergeAction(randomIntBetween(-10, 0), null));
        assertThat(r.getMessage(), equalTo("[max_num_segments] must be a positive integer"));
    }

    public void testInvalidCodec() {
        Exception r = expectThrows(IllegalArgumentException.class, () -> new
            ForceMergeAction(randomIntBetween(1, 10), Codec.forName("DummyCompressingStoredFields")));
        assertThat(r.getMessage(), equalTo("unknown index codec: [DummyCompressingStoredFields]"));
    }

    public void testToSteps() {
        ForceMergeAction instance = createTestInstance();
        if (instance.getCodec() != null && CodecService.BEST_COMPRESSION_CODEC.equals(instance.getCodec().getName())) {
            assertBestCompression(instance);
        }
        else {
            assertNonBestCompression(instance);
        }
    }
}
