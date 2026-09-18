/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class EsqlDatafeedSourceCheckpointTests extends AbstractXContentSerializingTestCase<EsqlDatafeedSourceCheckpoint> {

    private static final String JOB_ID = "job-1";
    private static final String DATAFEED_ID = "datafeed-1";

    @Override
    protected EsqlDatafeedSourceCheckpoint createTestInstance() {
        return new EsqlDatafeedSourceCheckpoint(
            randomAlphaOfLength(8),
            randomAlphaOfLength(8),
            randomLongBetween(0, 1_000_000),
            randomAlphaOfLength(64)
        );
    }

    @Override
    protected Writeable.Reader<EsqlDatafeedSourceCheckpoint> instanceReader() {
        return EsqlDatafeedSourceCheckpoint::new;
    }

    @Override
    protected EsqlDatafeedSourceCheckpoint doParseInstance(XContentParser parser) {
        return EsqlDatafeedSourceCheckpoint.PARSER.apply(parser, null);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));
    }

    @Override
    protected EsqlDatafeedSourceCheckpoint mutateInstance(EsqlDatafeedSourceCheckpoint instance) {
        return new EsqlDatafeedSourceCheckpoint(
            instance.getJobId() + "x",
            instance.getDatafeedId() + "x",
            instance.getSourceEndMs() + 1,
            instance.getFingerprint() + "x"
        );
    }

    public void testDocumentId() {
        assertThat(EsqlDatafeedSourceCheckpoint.documentId(JOB_ID), equalTo("job-1_esql_source_checkpoint"));
    }

    public void testComputeFingerprintShouldChangeWhenQueryShapeChanges() {
        String base = EsqlDatafeedSourceCheckpoint.computeFingerprint("FROM logs", "@timestamp", "bucket", TimeValue.timeValueHours(1));
        assertThat(
            EsqlDatafeedSourceCheckpoint.computeFingerprint("FROM other", "@timestamp", "bucket", TimeValue.timeValueHours(1)),
            not(equalTo(base))
        );
        assertThat(
            EsqlDatafeedSourceCheckpoint.computeFingerprint("FROM logs", "event.time", "bucket", TimeValue.timeValueHours(1)),
            not(equalTo(base))
        );
        assertThat(
            EsqlDatafeedSourceCheckpoint.computeFingerprint("FROM logs", "@timestamp", "time", TimeValue.timeValueHours(1)),
            not(equalTo(base))
        );
        assertThat(
            EsqlDatafeedSourceCheckpoint.computeFingerprint("FROM logs", "@timestamp", "bucket", TimeValue.timeValueMinutes(30)),
            not(equalTo(base))
        );
        assertThat(
            EsqlDatafeedSourceCheckpoint.computeFingerprint("FROM logs", "@timestamp", "bucket", TimeValue.timeValueHours(1)),
            equalTo(base)
        );
    }

    public void testComputeFingerprintShouldIgnoreOperationalKnobs() {
        DatafeedConfig.Builder base = new DatafeedConfig.Builder(DATAFEED_ID, JOB_ID).setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1));
        String fingerprint = EsqlDatafeedSourceCheckpoint.computeFingerprint(base.build(), "time");
        DatafeedConfig operational = new DatafeedConfig.Builder(DATAFEED_ID, JOB_ID).setEsqlQuery("FROM logs")
            .setSourceTimeField("@timestamp")
            .setGroupingInterval(TimeValue.timeValueHours(1))
            .setQueryDelay(TimeValue.timeValueMinutes(2))
            .setFrequency(TimeValue.timeValueMinutes(5))
            .setChunkingConfig(ChunkingConfig.newManual(TimeValue.timeValueHours(2)))
            .build();
        assertThat(EsqlDatafeedSourceCheckpoint.computeFingerprint(operational, "time"), equalTo(fingerprint));
    }

    public void testValidateFingerprintShouldDiscardMismatch() {
        EsqlDatafeedSourceCheckpoint checkpoint = new EsqlDatafeedSourceCheckpoint(JOB_ID, DATAFEED_ID, 1000L, "abc");
        assertThat(EsqlDatafeedSourceCheckpoint.validateFingerprint(checkpoint, "abc"), equalTo(checkpoint));
        assertThat(EsqlDatafeedSourceCheckpoint.validateFingerprint(checkpoint, "def"), nullValue());
        assertThat(EsqlDatafeedSourceCheckpoint.validateFingerprint(null, "abc"), nullValue());
    }
}
