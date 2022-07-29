/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.object.HasToString.hasToString;

public class NodeIndicesStatsTests extends ESTestCase {

    public void testInvalidLevel() {
        final NodeIndicesStats stats = new NodeIndicesStats(null, Collections.emptyMap());
        final String level = randomAlphaOfLength(16);
        final ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap("level", level));
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stats.toXContent(null, params));
        assertThat(
            e,
            hasToString(containsString("level parameter must be one of [indices] or [node] or [shards] but was [" + level + "]"))
        );
    }

}
