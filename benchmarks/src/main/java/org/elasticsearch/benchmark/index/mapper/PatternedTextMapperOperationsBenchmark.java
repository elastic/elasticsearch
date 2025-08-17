/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.elasticsearch.xpack.logsdb.patternedtext.PatternedTextValueProcessor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@State(Scope.Benchmark)
public class PatternedTextMapperOperationsBenchmark {

    @Param({
            "2020-09-06T08:29:04.123456",
           "2020-09-06T08:29:04.123Z",
           "2020-09-06T08:29:04,123",
           "2020-09-06T08:29:04.123+00:00",
           "2020-09-06T08:29:04Z",
           "2020-09-06T08:29:04+0000",
           "2020-09-06T08:29:04.123+0000",
           "2020-09-06 08:29:04,123",
           "2020-09-06 08:29:04.123",
           "2020-09-06 08:29:04",
           "2020/09/06 08:29:04",
           "06/Sep/2020:08:29:04 +0000",
           "2020-09-06 08:29:04 +0000",
           "2020-09-06 08:29:04 UTC",
           "06 Sep 2020 08:29:04.123"
        })
    private String timestampStr;


    @Benchmark
    public void testParseTimestamp(Blackhole blackhole) {

        String[] split = (timestampStr + " a a a a").split("[\\s\\[\\]]");
        blackhole.consume(PatternedTextValueProcessor.parse(split, 0));
    }

}
