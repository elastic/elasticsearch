/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.cli;

import com.carrotsearch.randomizedtesting.annotations.Listeners;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TimeUnits;
import org.elasticsearch.test.junit.listeners.LoggingListener;
import org.elasticsearch.test.junit.listeners.ReproduceInfoPrinter;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomAsciiLettersOfLength;

@Listeners({ ReproduceInfoPrinter.class, LoggingListener.class })
@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
@ThreadLeakLingering(linger = 5000) // 5 sec lingering
@TimeoutSuite(millis = 20 * TimeUnits.MINUTE)
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
// we suppress pretty much all the lucene codecs for now, except asserting
// assertingcodec is the winner for a codec here: it finds bugs and gives clear exceptions.
@LuceneTestCase.SuppressCodecs(
    {
        "SimpleText",
        "Memory",
        "CheapBastard",
        "Direct",
        "Compressing",
        "FST50",
        "FSTOrd50",
        "TestBloomFilteredLucenePostings",
        "MockRandom",
        "BlockTreeOrds",
        "LuceneFixedGap",
        "LuceneVarGapFixedInterval",
        "LuceneVarGapDocFreqInterval",
        "Lucene50" }
)
@LuceneTestCase.SuppressReproduceLine
public abstract class SqlCliTestCase extends LuceneTestCase {

    public boolean randomBoolean() {
        return random().nextBoolean();
    }

    public String randomAlphaOfLength(int length) {
        return randomAsciiLettersOfLength(length);
    }
}
