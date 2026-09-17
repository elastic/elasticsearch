/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.operator.exchange.BatchExchangeStatusResponse;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class RemoteFetchOperatorStatusTests extends AbstractWireSerializingTestCase<RemoteFetchOperator.Status> {
    @Override
    protected Writeable.Reader<RemoteFetchOperator.Status> instanceReader() {
        return RemoteFetchOperator.Status::new;
    }

    @Override
    protected RemoteFetchOperator.Status createTestInstance() {
        return new RemoteFetchOperator.Status(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomProfile()
        );
    }

    private static RemoteFetchOperator.Profile randomProfile() {
        return new RemoteFetchOperator.Profile(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected RemoteFetchOperator.Status mutateInstance(RemoteFetchOperator.Status in) throws IOException {
        int pagesReceived = in.pagesReceived();
        int pagesEmitted = in.pagesEmitted();
        long rowsReceived = in.rowsReceived();
        long rowsEmitted = in.rowsEmitted();
        long batchesSent = in.batchesSent();
        int exchangesOpened = in.exchangesOpened();
        RemoteFetchOperator.Profile profile = in.profile();
        switch (randomIntBetween(0, 6)) {
            case 0 -> pagesReceived = randomValueOtherThan(pagesReceived, ESTestCase::randomNonNegativeInt);
            case 1 -> pagesEmitted = randomValueOtherThan(pagesEmitted, ESTestCase::randomNonNegativeInt);
            case 2 -> rowsReceived = randomValueOtherThan(rowsReceived, ESTestCase::randomNonNegativeLong);
            case 3 -> rowsEmitted = randomValueOtherThan(rowsEmitted, ESTestCase::randomNonNegativeLong);
            case 4 -> batchesSent = randomValueOtherThan(batchesSent, ESTestCase::randomNonNegativeLong);
            case 5 -> exchangesOpened = randomValueOtherThan(exchangesOpened, ESTestCase::randomNonNegativeInt);
            case 6 -> profile = randomValueOtherThan(profile, RemoteFetchOperatorStatusTests::randomProfile);
            default -> throw new UnsupportedOperationException();
        }
        return new RemoteFetchOperator.Status(
            pagesReceived,
            pagesEmitted,
            rowsReceived,
            rowsEmitted,
            batchesSent,
            exchangesOpened,
            profile
        );
    }

    public void testToXContent() {
        RemoteFetchOperator.Status status = new RemoteFetchOperator.Status(
            1,
            2,
            30,
            20,
            4,
            3,
            new RemoteFetchOperator.Profile(100, 50, 25, 80, 10, 20, 15, 70, 40, 30, 60, 25, 12, 24, 1024, 2048)
        );
        assertThat(Strings.toString(status), equalTo("""
            {"pages_received":1,"pages_emitted":2,"rows_received":30,"rows_emitted":20,"batches_sent":4,"exchanges_opened":3,\
            "process_nanos":100,"time_to_first_result_nanos":50,"response_nanos":25,"exchange_wait_nanos":80,"merge_nanos":10,\
            "setup_nanos":20,"max_setup_nanos":15,\
            "fetch_nanos":70,"max_fetch_nanos":40,"fetch_cpu_nanos":30,"field_load_nanos":60,"values_loaded":25,\
            "source_docs_loaded":12,"source_field_reads":24,"source_bytes_loaded":1024,"bytes_read":2048}"""));
    }

    public void testProfileIsOmittedForOldVersions() throws IOException {
        RemoteFetchOperator.Status original = createTestInstance();
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(BatchExchangeStatusResponse.ESQL_BATCH_EXCHANGE_PROFILE);
        BytesReference bytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(oldVersion);
            original.writeTo(out);
            bytes = out.bytes();
        }

        RemoteFetchOperator.Status deserialized;
        try (StreamInput in = bytes.streamInput()) {
            in.setTransportVersion(oldVersion);
            deserialized = new RemoteFetchOperator.Status(in);
        }
        assertThat(
            deserialized,
            equalTo(
                new RemoteFetchOperator.Status(
                    original.pagesReceived(),
                    original.pagesEmitted(),
                    original.rowsReceived(),
                    original.rowsEmitted(),
                    original.batchesSent(),
                    original.exchangesOpened()
                )
            )
        );
    }
}
