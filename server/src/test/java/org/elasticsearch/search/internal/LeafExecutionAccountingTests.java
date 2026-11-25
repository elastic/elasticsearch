/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.search.internal.BreakerWeightTestUtils.TrackingCircuitBreaker;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.equalTo;

/** Checks that settling a {@link LeafExecutionAccounting#reserve} releases from the leaf or {@link LeafExecutionScope} it was attributed to. */
public class LeafExecutionAccountingTests extends ESTestCase {

    private static final String LABEL = "test";
    private static final long CHARGED = 1000L;
    private static final long RETAINED = 300L;

    private Directory directory;
    private DirectoryReader reader;
    private LeafReaderContext leaf;
    private TrackingCircuitBreaker breaker;
    private LeafExecutionAccounting accounting;

    @Before
    public void initAccounting() throws IOException {
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            writer.addDocument(new Document());
        }
        reader = DirectoryReader.open(directory);
        leaf = reader.leaves().getFirst();
        breaker = new TrackingCircuitBreaker(-1L);
        accounting = new LeafExecutionAccounting(breaker, reader.leaves().size());
    }

    @After
    public void closeAccounting() throws IOException {
        accounting.close();
        assertThat(breaker.getUsed(), equalTo(0L));
        IOUtils.close(reader, directory);
    }

    public void testSettleOnLeafReleasesTheExcess() {
        accounting.reserve(leaf, CHARGED, LABEL).accept(RETAINED);
        assertThat(breaker.getUsed(), equalTo(RETAINED));
        accounting.enterLeaf(leaf).close();
        assertThat("the leaf release must drain only what is left after the settle", breaker.getUsed(), equalTo(0L));
    }

    public void testSettleInScopeReleasesFromTheScope() {
        LeafExecutionScope scope = new LeafExecutionScope();
        LongConsumer settle = scope.capture(() -> accounting.reserve(leaf, CHARGED, LABEL));
        settle.accept(RETAINED);
        assertThat(breaker.getUsed(), equalTo(RETAINED));
        accounting.enterLeaf(leaf).close();
        assertThat("the scope, not the leaf, holds what is left after the settle", breaker.getUsed(), equalTo(RETAINED));
        scope.release();
        assertThat("the scope must release only what is left after the settle", breaker.getUsed(), equalTo(0L));
    }

    public void testSettleNeverRaisesTheCharge() {
        LongConsumer settle = accounting.reserve(leaf, CHARGED, LABEL);
        settle.accept(randomLongBetween(CHARGED, Long.MAX_VALUE));
        assertThat(breaker.getUsed(), equalTo(CHARGED));
    }

    public void testSettleOnNothingRetainedReleasesEverything() {
        accounting.reserve(leaf, CHARGED, LABEL).accept(0L);
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testSettleAfterLeafReleaseReleasesNothing() {
        LongConsumer settle = accounting.reserve(leaf, CHARGED, LABEL);
        accounting.enterLeaf(leaf).close();
        settle.accept(RETAINED);
        assertThat("a settle after the release must not release again", breaker.getUsed(), equalTo(0L));
    }

    public void testSettleAfterScopeReleaseReleasesNothing() {
        LeafExecutionScope scope = new LeafExecutionScope();
        LongConsumer settle = scope.capture(() -> accounting.reserve(leaf, CHARGED, LABEL));
        scope.release();
        settle.accept(RETAINED);
        assertThat("a settle after the release must not release again", breaker.getUsed(), equalTo(0L));
    }

    public void testSettleOnLeafLeavesOtherChargesOnTheLeaf() {
        LongConsumer settleFirst = accounting.reserve(leaf, CHARGED, LABEL);
        accounting.charge(leaf, CHARGED, LABEL);
        settleFirst.accept(RETAINED);
        assertThat(breaker.getUsed(), equalTo(CHARGED + RETAINED));
    }

    public void testNoChargeSettlesNothing() {
        assertNull(accounting.reserve(leaf, 0L, LABEL));
        assertThat(breaker.getUsed(), equalTo(0L));
    }
}
