/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.SkipWarnings;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The CSV/NDJSON analog of {@code ExternalParquetCoercionWarningFloodIT}, at the unit level: a lenient
 * ({@code null_field} / {@code skip_row}) external read of a multi-file glob or macro-split text source
 * fans out into many units spread across parallel drivers, and its client-visible {@code Warning} channel
 * must stay bounded no matter how many units or drivers the read fans into.
 *
 * <p>This models the real two-layer bound the async source applies:
 * <ul>
 *   <li>{@link AsyncExternalSourceBuffer#recordInformationalWarning} caps each driver's own buffer, so a
 *       single driver's chunks/segments cannot flood on their own.</li>
 *   <li>One buffer is created per driver, so that per-buffer cap alone still lets the total grow with the
 *       driver count. {@code AsyncExternalSourceOperatorFactory} therefore gates every informational sink
 *       through one shared {@link InformationalWarningBudget} so the ceiling holds per source per node.</li>
 * </ul>
 * The per-buffer cap is what a single-driver read needs; the shared budget is what a fanned-out read needs.
 * This test exercises the fanned-out case: several per-driver buffers, each already capped, all sharing one
 * budget, with real per-unit {@link SkipWarnings} carrying the exact summary and per-value detail text
 * {@code CsvFormatReader} / {@code NdJsonPageDecoder} emit. Removing the shared budget leaves the total at
 * {@code drivers x per-buffer-cap}, which is the cross-driver flood the per-buffer cap does not cover.
 */
public class ExternalReaderWarningFloodTests extends ESTestCase {

    /** Parallel drivers a fanned-out read uses, each with its own {@link AsyncExternalSourceBuffer}. */
    private static final int DRIVERS = 16;
    /** Units (chunks/segments/globbed files) each driver processes; each builds its own {@link SkipWarnings}. */
    private static final int UNITS_PER_DRIVER = 4;
    /** Distinct unparseable values per unit; above the per-instance cap so each unit alone already overflows. */
    private static final int BAD_ROWS_PER_UNIT = 40;

    public void testCsvNullFieldWarningsStayBoundedAcrossDriverFanOut() {
        assertChannelBounded("CSV", "affected rows/fields are listed below");
    }

    public void testNdjsonNullFieldWarningsStayBoundedAcrossDriverFanOut() {
        assertChannelBounded("NDJSON", "affected rows are listed below");
    }

    /**
     * Replays {@link #DRIVERS} drivers, each draining its own buffer, every driver processing
     * {@link #UNITS_PER_DRIVER} reader units that each emit {@link #BAD_ROWS_PER_UNIT} distinct null-fill
     * warnings of the given format's shape through the one shared budget. Unions the drained per-driver
     * headers the way the coordinator unions per-driver response headers, then asserts the drift still
     * surfaces and that the whole channel stays within {@code MAX_ADDED_WARNINGS + 1} regardless of the
     * driver, unit, and per-unit row counts.
     */
    private void assertChannelBounded(String format, String summaryTail) {
        int cap = SkipWarnings.MAX_ADDED_WARNINGS;
        InformationalWarningBudget budget = new InformationalWarningBudget(cap);

        List<String> delivered = new ArrayList<>();
        for (int driver = 0; driver < DRIVERS; driver++) {
            AsyncExternalSourceBuffer buffer = new AsyncExternalSourceBuffer(AsyncExternalSourceBuffer.DEFAULT_MAX_BUFFER_BYTES);
            // Exactly what AsyncExternalSourceOperatorFactory#bufferedInformationalWarningSink does: gate every
            // reader warning through the one per-source budget, then record survivors on this driver's buffer
            // (which additionally caps per buffer) for the driver thread to re-emit on close.
            Consumer<String> sink = warning -> {
                String toRecord = budget.accept(warning);
                if (toRecord != null) {
                    buffer.recordInformationalWarning(toRecord);
                }
            };

            for (int unit = 0; unit < UNITS_PER_DRIVER; unit++) {
                // One collector per unit, mirroring the reader's per-chunk/per-file SkipWarnings. The summary
                // embeds a per-unit source location, as a multi-file glob would, so summaries are distinct too.
                String file = "part-" + driver + "-" + unit + "." + format.toLowerCase(Locale.ROOT);
                String summary = format
                    + " read from ["
                    + file
                    + "] "
                    + "encountered parse errors handled per policy (policy: null_field); "
                    + summaryTail;
                SkipWarnings unitWarnings = SkipWarnings.of(ErrorPolicy.PERMISSIVE, summary, sink);
                for (int row = 0; row < BAD_ROWS_PER_UNIT; row++) {
                    unitWarnings.add("Failed to parse " + format + " value [notanumber-" + driver + "-" + unit + "-" + row + "] as [LONG]");
                }
            }

            // Drain this driver's buffer the way AsyncExternalSourceOperator#close() does, then union into the
            // response-wide set as the coordinator would.
            String warning;
            while ((warning = buffer.pollWarning()) != null) {
                delivered.add(warning);
            }
        }

        assertThat(
            "the null-fill drift must still surface at least one per-value warning, got: " + delivered,
            delivered,
            hasItem(containsString("as [LONG]"))
        );
        // Sanity: the fixture presents far more distinct warnings than the cap, so a passing bound is a real
        // cap and not an artifact of too little input.
        assertThat((long) DRIVERS * UNITS_PER_DRIVER * BAD_ROWS_PER_UNIT, greaterThan((long) cap + 1));
        assertThat(
            "the informational Warning channel must stay bounded across the whole driver fan-out (drivers="
                + DRIVERS
                + ", units/driver="
                + UNITS_PER_DRIVER
                + ", rows/unit="
                + BAD_ROWS_PER_UNIT
                + "), got "
                + delivered.size()
                + ": "
                + delivered,
            delivered.size(),
            lessThanOrEqualTo(cap + 1)
        );
    }
}
