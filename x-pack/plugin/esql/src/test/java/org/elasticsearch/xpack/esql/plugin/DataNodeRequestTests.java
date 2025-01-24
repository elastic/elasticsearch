/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.security.authz.IndicesAndAliasesResolverField.NO_INDEX_PLACEHOLDER;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class DataNodeRequestTests extends ESTestCase {

    public void testNoIndexPlaceholder() {
        var sessionId = randomAlphaOfLength(10);
        List<ShardId> shardIds = randomList(1, 10, () -> new ShardId("index-" + between(1, 10), "n/a", between(1, 10)));

        DataNodeRequest request = new DataNodeRequest(
            sessionId,
            randomConfiguration("""
                from test
                | where round(emp_no) > 10
                | eval c = salary
                | stats x = avg(c)
                """, randomTables()),
            randomAlphaOfLength(10),
            shardIds,
            Collections.emptyMap(),
            null,
            generateRandomStringArray(10, 10, false, false),
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()),
            randomBoolean()
        );

        assertThat(request.shardIds(), equalTo(shardIds));

        request.indices(generateRandomStringArray(10, 10, false, false));

        assertThat(request.shardIds(), equalTo(shardIds));

        request.indices(NO_INDEX_PLACEHOLDER);

        assertThat(request.shardIds(), empty());
    }
}
