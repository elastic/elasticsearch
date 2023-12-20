/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.fs;

import org.elasticsearch.monitor.fs.FsInfo.Path;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class FsInfoTests extends ESTestCase {

    public void testTotalSummation() {
        Path p1 = new Path("J:\\data\\nodes\\0", "Disk10 (J:)", 4, 3, 2);
        Path p2 = new Path("K:\\data\\nodes\\0", "Disk11 (K:)", 8, 6, 4);
        FsInfo info = new FsInfo(-1, null, new Path[] { p1, p2 });

        Path total = info.getTotal();
        assertThat(total.getTotal().getBytes(), is(12L));
        assertThat(total.getFree().getBytes(), is(9L));
        assertThat(total.getAvailable().getBytes(), is(6L));
    }

    public void testTotalDeduplication() {
        Path p1 = new Path("/app/data/es-1", "/app (/dev/sda1)", 8, 6, 4);
        Path p2 = new Path("/app/data/es-2", "/app (/dev/sda1)", 8, 6, 4);
        FsInfo info = new FsInfo(-1, null, new Path[] { p1, p2 });

        Path total = info.getTotal();
        assertThat(total.getTotal().getBytes(), is(8L));
        assertThat(total.getFree().getBytes(), is(6L));
        assertThat(total.getAvailable().getBytes(), is(4L));
    }

}
