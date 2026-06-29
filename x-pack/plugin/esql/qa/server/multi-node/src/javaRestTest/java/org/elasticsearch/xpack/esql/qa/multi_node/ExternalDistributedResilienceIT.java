/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import fixture.s3.BlobEntry;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.xpack.esql.datasources.FaultInjectingS3HttpHandler;
import org.elasticsearch.xpack.esql.datasources.FaultInjectingS3HttpHandler.FaultType;
import org.junit.After;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.util.TestUtils.isServerless;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.BUCKET;
import static org.elasticsearch.xpack.esql.datasources.S3FixtureUtils.WAREHOUSE;

/**
 * Integration tests verifying that distributed external source queries recover from
 * transient S3 faults (503, 500, connection reset) via the retry policy, and fail
 * cleanly when faults exceed the retry budget.
 */
@SuppressForbidden(reason = "uses FaultInjectingS3HttpHandler which wraps HttpHandler for S3 fault injection")
@ThreadLeakFilters(filters = { TestClustersThreadFilter.class })
public class ExternalDistributedResilienceIT extends AbstractExternalDistributedIT {

    @After
    public void clearFaults() {
        faultHandler().clearFault();
    }

    private FaultInjectingS3HttpHandler faultHandler() {
        return s3Fixture.getFaultHandler();
    }

    private String employeesQuery() throws IOException {
        return fromS3(WAREHOUSE + "/standalone/employees.parquet") + " | KEEP emp_no, first_name, salary | SORT emp_no | LIMIT 5";
    }

    public void testNoFaultQuerySucceeds() throws Exception {
        s3Fixture.loadFixturesFromResources();
        faultHandler().clearFault();

        for (String mode : DISTRIBUTION_MODES) {
            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values in result for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results for mode %s", mode), values.isEmpty());
        }
    }

    public void testTransient503RecoveryDuringDistributedQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_503, 2);

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after 503 recovery for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results after 503 recovery for mode %s", mode), values.isEmpty());
            assertEquals(Strings.format("All faults should have been consumed for mode %s", mode), 0, faultHandler().remainingFaults());
        }
    }

    public void testTransient500RecoveryDuringDistributedQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_500, 2);

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after 500 recovery for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results after 500 recovery for mode %s", mode), values.isEmpty());
            assertEquals(Strings.format("All faults should have been consumed for mode %s", mode), 0, faultHandler().remainingFaults());
        }
    }

    public void testConnectionResetRecoveryDuringDistributedQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.CONNECTION_RESET, 2);

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after connection reset recovery for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results after connection reset recovery for mode %s", mode), values.isEmpty());
            assertEquals(Strings.format("All faults should have been consumed for mode %s", mode), 0, faultHandler().remainingFaults());
        }
    }

    public void testPersistentFaultsFailQuery() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            // Global per-request fault budget; distributed Parquet reads issue many HTTP requests and retries.
            faultHandler().setFault(FaultType.HTTP_503, isServerless(adminClient()) ? 1_000 : 100);

            Exception ex = expectThrows(Exception.class, () -> runQueryWithMode(employeesQuery(), mode));
            if (ex instanceof ResponseException responseEx) {
                String responseBody = new String(responseEx.getResponse().getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
                assertTrue(
                    Strings.format("Expected storage error in response for mode %s, got: %s", mode, responseBody),
                    responseBody.contains("503")
                        || responseBody.contains("Service Unavailable")
                        || responseBody.contains("SlowDown")
                        || responseBody.contains("storage")
                        || responseBody.contains("error")
                );
            } else {
                // A Parquet read involves multiple sequential S3 operations, each with its own
                // retry budget. Under persistent throttling the cumulative retry time can exceed
                // the REST client's socket timeout, which is an acceptable failure mode.
                assertTrue(
                    Strings.format("Expected ResponseException or SocketTimeoutException for mode %s, got: %s", mode, ex.getClass()),
                    hasCause(ex, SocketTimeoutException.class)
                );
            }

            faultHandler().clearFault();
        }
    }

    /**
     * End-to-end coverage of the <b>multi-segment</b> uncompressed external-read path (the path whose
     * idle-socket reset this change fixes). The file is sized above twice NDJSON's 4 MiB minimum segment size, so
     * {@code parsing_parallelism >= 2} splits it into several byte-range segments and the read goes
     * through {@code ParallelParsingCoordinator}'s multi-segment as-ready iterator rather than the
     * single-stream fallback. With the S3 fixture injecting connection resets on the data object's
     * reads, the query must still return every row exactly once through the real
     * REST/planner/executor/storage stack.
     * <p>
     * Note on layering: the fixture's {@link FaultType#CONNECTION_RESET} drops the connection at
     * stream-open, which the object-store layer retries transparently — so this asserts integrated
     * correctness + open-time resilience for the multi-segment path. The mid-read path (a reset on an
     * already-open stream) is exercised end-to-end by
     * {@link #testMultiSegmentNdjsonReadRecoversFromMidReadReset} and deterministically at the unit layer by
     * {@code RetryableStorageObjectTests#testRangeReadResumesByteExactAfterMidReadFault}.
     */
    public void testMultiSegmentNdjsonReadRecoversFromConnectionReset() throws Exception {
        int rows = 200_000;
        // ~16 MiB of uncompressed NDJSON: comfortably above 2x the 4 MiB minimum segment size, so a
        // parsing_parallelism of 4 yields multiple segments.
        byte[] ndjson = generateNdjson(rows);
        for (String mode : DISTRIBUTION_MODES) {
            // A distinct object per mode so every read is a cold multi-segment scan: external-text aggregate
            // pushdown (#149380) would short-circuit a warm re-read of a shared path from cached source stats and
            // skip the faulted streaming read. See testMultiSegmentNdjsonReadRecoversFromMidReadReset.
            String fileName = "big-" + mode + ".ndjson";
            String key = WAREHOUSE + "/reset/" + fileName;
            s3Fixture.getHandler().blobs().put("/" + BUCKET + "/" + key, new BlobEntry(new BytesArray(ndjson), "STANDARD"));

            String query = fromS3(key) + " | STATS count = COUNT(*)";
            // Two resets on the data object's reads; each segment re-open is within the retry budget.
            faultHandler().setFault(FaultType.CONNECTION_RESET, 2, path -> path.endsWith(fileName));

            Map<String, Object> result = runQueryWithMode(query, mode, 4);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after reset recovery for mode %s", mode), values);
            assertEquals(Strings.format("Expected a single COUNT row for mode %s", mode), 1, values.size());
            assertEquals(
                Strings.format("Expected exactly-once full count after reset recovery for mode %s", mode),
                (long) rows,
                ((Number) values.get(0).get(0)).longValue()
            );
            assertEquals(
                Strings.format("All injected resets should have been consumed for mode %s", mode),
                0,
                faultHandler().remainingFaults()
            );

            faultHandler().clearFault();
        }
    }

    /**
     * The strongest exactly-once check: run a multi-aggregate query clean, then run the identical query with a
     * mid-read reset injected, and assert the faulted result is byte-for-byte equal to the clean baseline. If a
     * resume dropped or replayed a byte, COUNT/SUM/MAX would diverge and the comparison fails.
     */
    public void testFaultedMidReadResultEqualsCleanBaseline() throws Exception {
        int rows = 200_000;
        byte[] ndjson = generateNdjson(rows);
        for (String mode : DISTRIBUTION_MODES) {
            // Two distinct objects with identical content per mode so BOTH the clean and the faulted run are cold
            // streaming scans. Sharing one path would let external-text aggregate pushdown (#149380) answer the
            // second (faulted) run from the cache the clean run warmed — skipping the read the fault targets.
            String cleanFile = "clean-" + mode + ".ndjson";
            String faultedFile = "faulted-" + mode + ".ndjson";
            String cleanKey = WAREHOUSE + "/baseline/" + cleanFile;
            String faultedKey = WAREHOUSE + "/baseline/" + faultedFile;
            s3Fixture.getHandler().blobs().put("/" + BUCKET + "/" + cleanKey, new BlobEntry(new BytesArray(ndjson), "STANDARD"));
            s3Fixture.getHandler().blobs().put("/" + BUCKET + "/" + faultedKey, new BlobEntry(new BytesArray(ndjson), "STANDARD"));

            String statsTail = " | STATS c = COUNT(*), s = SUM(salary), m = MAX(salary)";
            @SuppressWarnings("unchecked")
            List<List<Object>> clean = (List<List<Object>>) runQueryWithMode(fromS3(cleanKey) + statsTail, mode, 4).get("values");

            faultHandler().setMidBodyResetFault(2, 256, path -> path.endsWith(faultedFile));
            @SuppressWarnings("unchecked")
            List<List<Object>> faulted = (List<List<Object>>) runQueryWithMode(fromS3(faultedKey) + statsTail, mode, 4).get("values");

            assertEquals(Strings.format("faulted result must equal the clean baseline for mode %s", mode), clean, faulted);
            assertEquals(Strings.format("all injected resets consumed for mode %s", mode), 0, faultHandler().remainingFaults());
            faultHandler().clearFault();
        }
    }

    /**
     * End-to-end regression for the idle/connection reset on an already-open external segment stream. Unlike
     * {@link #testMultiSegmentNdjsonReadRecoversFromConnectionReset} (which resets at stream-open, where the
     * object-store layer retries), this drops the connection <b>mid-body</b> on a segment read — a reset on an
     * already-open stream, which the self-healing storage read ({@code RetryableStorageObject}'s resuming
     * stream) recovers beneath the coordinator by re-opening the remaining byte range and resuming byte-exact.
     * The threshold is set above the small schema/record-boundary probes so only the large segment read is
     * faulted. Before the fix this failed the query with "Parallel parsing failed" caused by a
     * {@link java.net.SocketException}; the full count coming back proves exactly-once recovery through the
     * real stack.
     */
    public void testMultiSegmentNdjsonReadRecoversFromMidReadReset() throws Exception {
        int rows = 500_000;
        // ~42 MiB so parsing_parallelism=4 yields ~10 MiB segments — each segment read streams far more than
        // the 1 MiB reset threshold, while record-boundary probes (a few KiB) stay under it.
        byte[] ndjson = generateNdjson(rows);
        for (String mode : DISTRIBUTION_MODES) {
            // A distinct object per mode so every read is a cold multi-segment scan. External-text aggregate
            // pushdown (#149380) short-circuits a warm re-read of the same path from cached source stats —
            // skipping the streaming read this test injects the mid-body fault into — so a shared key would let a
            // later mode (whichever routes to the coordinator the first mode warmed) never hit the fault.
            String fileName = "big-" + mode + ".ndjson";
            String key = WAREHOUSE + "/reset-midbody/" + fileName;
            s3Fixture.getHandler().blobs().put("/" + BUCKET + "/" + key, new BlobEntry(new BytesArray(ndjson), "STANDARD"));

            // Reference a column (salary) so the read does NOT take the empty-projection schema-bind, which streams
            // the whole file as one object read during setup. With a projected column the fault lands on a segment
            // byte-range read — exactly the newStream(pos,len) range the storage layer re-opens and resumes.
            // max(salary) is deterministic: salary = 40000 + (i % 50000), so the max over 500k rows is 89999.
            String query = fromS3(key) + " | STATS count = COUNT(*), max_salary = MAX(salary)";
            faultHandler().setMidBodyResetFault(1, 1024 * 1024, path -> path.endsWith(fileName));

            Map<String, Object> result = runQueryWithMode(query, mode, 4);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after mid-read reset recovery for mode %s", mode), values);
            assertEquals(Strings.format("Expected a single result row for mode %s", mode), 1, values.size());
            assertEquals(
                Strings.format("Expected exactly-once full count after mid-read reset recovery for mode %s", mode),
                (long) rows,
                ((Number) values.get(0).get(0)).longValue()
            );
            assertEquals(
                Strings.format("Expected the full salary range to survive reset recovery for mode %s", mode),
                89999L,
                ((Number) values.get(0).get(1)).longValue()
            );
            assertTrue(
                Strings.format("Expected the injected mid-read reset to fire for mode %s", mode),
                faultHandler().remainingFaults() <= 0
            );

            faultHandler().clearFault();
        }
    }

    /**
     * Cross-format proof that the self-heal lives below the format boundary: a mid-read connection drop on a
     * <b>parquet</b> object is recovered exactly like a text segment, because parquet's footer and row-group
     * reads go through the same {@code StorageObject.newStream(position, length)} path. The reset threshold is
     * small (the parquet fixture is small); the storage layer re-opens the byte range and resumes, so the read
     * — and the query — still succeed despite the drop. Parquet never goes through the text coordinator, so
     * this is covered solely by the storage-layer self-heal.
     */
    public void testParquetReadRecoversFromMidReadReset() throws Exception {
        s3Fixture.loadFixturesFromResources();
        // A projecting + sorting query reads row-group column data — not just footer metadata, which is all
        // COUNT(*) would touch — so a read large enough for the mid-body fault to land on is guaranteed.
        String query = employeesQuery();
        for (String mode : DISTRIBUTION_MODES) {
            // Drop the connection after 128 bytes of a parquet column-data read, past the tiny magic / footer-
            // length reads. The storage layer re-opens the byte range and resumes.
            faultHandler().setMidBodyResetFault(1, 128, path -> path.endsWith(".parquet"));

            Map<String, Object> result = runQueryWithMode(query, mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected rows after parquet mid-read recovery for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty parquet results after recovery for mode %s", mode), values.isEmpty());
            assertTrue(
                Strings.format("Expected the injected mid-read reset to fire for mode %s", mode),
                faultHandler().remainingFaults() <= 0
            );

            faultHandler().clearFault();
        }
    }

    private static byte[] generateNdjson(int rows) {
        StringBuilder sb = new StringBuilder(rows * 90);
        for (int i = 0; i < rows; i++) {
            sb.append("{\"id\":")
                .append(i)
                .append(",\"name\":\"emp-")
                .append(i)
                .append("\",\"dept\":\"engineering-department-")
                .append(i % 32)
                .append("\",\"salary\":")
                .append(40000 + (i % 50000))
                .append("}\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
        }
        return false;
    }

    public void testPathFilteredFaultsOnlyAffectParquetReads() throws Exception {
        s3Fixture.loadFixturesFromResources();

        for (String mode : DISTRIBUTION_MODES) {
            faultHandler().setFault(FaultType.HTTP_503, 2, path -> path.endsWith(".parquet"));

            Map<String, Object> result = runQueryWithMode(employeesQuery(), mode);
            @SuppressWarnings("unchecked")
            List<List<Object>> values = (List<List<Object>>) result.get("values");
            assertNotNull(Strings.format("Expected values after path-filtered fault recovery for mode %s", mode), values);
            assertFalse(Strings.format("Expected non-empty results for mode %s", mode), values.isEmpty());
        }
    }
}
