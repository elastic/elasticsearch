/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ExternalFieldExtractOperatorStatusTests extends AbstractWireSerializingTestCase<ExternalFieldExtractOperator.Status> {

    @Override
    protected Writeable.Reader<ExternalFieldExtractOperator.Status> instanceReader() {
        return ExternalFieldExtractOperator.Status::new;
    }

    @Override
    protected ExternalFieldExtractOperator.Status createTestInstance() {
        return new ExternalFieldExtractOperator.Status(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected ExternalFieldExtractOperator.Status mutateInstance(ExternalFieldExtractOperator.Status instance) {
        long pages = instance.pagesProcessed();
        long rows = instance.rowsEmitted();
        long nanos = instance.extractNanos();
        long cpuNanos = instance.readCpuNanos();
        switch (between(0, 3)) {
            case 0 -> pages = randomValueOtherThan(pages, ESTestCase::randomNonNegativeLong);
            case 1 -> rows = randomValueOtherThan(rows, ESTestCase::randomNonNegativeLong);
            case 2 -> nanos = randomValueOtherThan(nanos, ESTestCase::randomNonNegativeLong);
            case 3 -> cpuNanos = randomValueOtherThan(cpuNanos, ESTestCase::randomNonNegativeLong);
        }
        return new ExternalFieldExtractOperator.Status(pages, rows, nanos, cpuNanos);
    }

    public void testToXContent() {
        ExternalFieldExtractOperator.Status status = new ExternalFieldExtractOperator.Status(12, 4096, 1_500_000, 1_200_000);
        assertThat(
            Strings.toString(status),
            equalTo(
                "{\"process_nanos\":1500000,\"pages_received\":12,\"pages_completed\":12,\"pages_processed\":12,"
                    + "\"rows_extracted\":4096,\"extract_nanos\":1500000,\"extract_cpu_nanos\":1200000}"
            )
        );
    }

    public void testReadFromBwcVersionPriorToProfile() throws IOException {
        ExternalFieldExtractOperator.Status original = new ExternalFieldExtractOperator.Status(12, 4096, 1_500_000, 1_200_000);
        TransportVersion preProfile = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_external_source_profile"));
        ExternalFieldExtractOperator.Status copy = copyInstance(original, preProfile);
        // Pre-profile nodes never produced this Status entry, but be defensive: round-tripping
        // through an older wire-version yields zero counters rather than failing.
        assertThat(copy.pagesProcessed(), equalTo(0L));
        assertThat(copy.rowsEmitted(), equalTo(0L));
        assertThat(copy.extractNanos(), equalTo(0L));
        assertThat(copy.readCpuNanos(), equalTo(0L));
    }

    public void testReadFromBwcVersionPriorToExtractCpuNanos() throws IOException {
        ExternalFieldExtractOperator.Status original = new ExternalFieldExtractOperator.Status(12, 4096, 1_500_000, 1_200_000);
        TransportVersion preExtractCpu = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_extract_cpu_nanos"));
        ExternalFieldExtractOperator.Status copy = copyInstance(original, preExtractCpu);
        assertThat(copy.pagesProcessed(), equalTo(12L));
        assertThat(copy.extractNanos(), equalTo(1_500_000L));
        assertThat(copy.readCpuNanos(), equalTo(0L));
    }
}
