/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class MockPageCacheRecyclerTests extends ESTestCase {

    public void testDoubleReleaseThrows() {
        final var recycler = new MockPageCacheRecycler(Settings.EMPTY);
        final var page = recycler.bytePage(randomBoolean());
        page.close();
        final var e = expectThrows(IllegalStateException.class, page::close);
        assertThat(e.getMessage(), containsString("Double release"));
    }

    public void testWrapperDelegatesAndTracks() throws Exception {
        final var inner = new PageCacheRecycler(Settings.EMPTY);
        final var recycler = MockPageCacheRecycler.wrap(inner);

        // Wrapping an already-wrapped recycler must be a no-op.
        assertSame(recycler, MockPageCacheRecycler.wrap(recycler));

        final var page = recycler.bytePage(randomBoolean());
        final var leak = expectThrows(RuntimeException.class, MockPageCacheRecycler::ensureAllPagesAreReleased);
        assertThat(leak.getMessage(), containsString("pages have not been released"));

        page.close();

        MockPageCacheRecycler.ensureAllPagesAreReleased();
    }
}
