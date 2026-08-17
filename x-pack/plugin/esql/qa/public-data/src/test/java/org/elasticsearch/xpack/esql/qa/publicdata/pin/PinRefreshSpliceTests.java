/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.publicdata.pin;

import org.elasticsearch.test.ESTestCase;

import java.util.Map;

/** The --write splice must replace exactly the matched variant's pin block and nothing else. */
public class PinRefreshSpliceTests extends ESTestCase {

    private static final String CATALOG = """
        corpora:
          - id: c1
            variants:
              # comment that must survive
              - provider: s3
                resource: "s3://b/one.parquet"
                pin:
                  method: HEAD
                  verified_at: 2026-01-01T00:00:00Z
                  object_count: 1
                  total_bytes: 1
                  samples:
                    - key: "one.parquet"
                      size: 1
              - provider: s3
                resource: "s3://b/two.parquet"
                pin:
                  method: HEAD
                  verified_at: 2026-01-01T00:00:00Z
                  object_count: 1
                  total_bytes: 2
                  samples:
                    - key: "two.parquet"
                      size: 2
        """;

    public void testSpliceReplacesOnlyTheMatchedPin() {
        String refreshed = """
            pin:
              method: HEAD
              verified_at: 2026-08-14T00:00:00Z
              object_count: 1
              total_bytes: 111
              samples:
                - key: "one.parquet"
                  size: 111
            """;
        String out = PinRefreshCli.spliceRefreshedPins(CATALOG, Map.of("s3://b/one.parquet", refreshed));
        assertTrue(out.contains("total_bytes: 111"));
        assertTrue("second pin untouched", out.contains("total_bytes: 2"));
        assertTrue("comment survives", out.contains("# comment that must survive"));
        assertTrue("verified_at updated", out.contains("2026-08-14T00:00:00Z"));
        assertTrue("old bytes gone", out.contains("total_bytes: 1\n") == false);
        // indentation preserved: the refreshed pin body sits two deeper than the pin: key, which
        // the text block places at 8 spaces (after common-indent stripping) -> body at 10
        assertTrue(out.contains("\n          total_bytes: 111\n"));
    }

    public void testUnknownResourceLeavesTextUntouched() {
        String out = PinRefreshCli.spliceRefreshedPins(CATALOG, Map.of("s3://b/absent.parquet", "pin:\n  method: HEAD\n"));
        assertEquals(CATALOG.stripTrailing() + "\n", out);
    }
}
