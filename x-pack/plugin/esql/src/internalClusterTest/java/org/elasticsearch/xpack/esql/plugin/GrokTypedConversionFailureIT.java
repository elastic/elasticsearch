/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;

/**
 * A GROK typed capture (eg. {@code %{NUMBER:n:int}}) matching indexed data it cannot convert must
 * not fail the shard: the offending row gets null extracted values (plus a warning) and all other
 * rows are extracted normally. This used to escape as an {@code EsqlClientException} ("For input
 * string: ...") that failed the whole shard.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class GrokTypedConversionFailureIT extends AbstractEsqlIntegTestCase {

    public void testTypedCaptureConversionFailureDoesNotFailShard() {
        prepareIndex("test").setId("1").setSource("message", "42").get();
        prepareIndex("test").setId("2").setSource("message", "1.5").get();
        refresh("test");

        try (EsqlQueryResponse response = run("FROM test | GROK message \"%{NUMBER:n:int}\" | SORT message | KEEP n")) {
            List<Object> values = new ArrayList<>();
            response.values().forEachRemaining(row -> values.add(row.next()));
            assertThat(values, contains((Object) null, 42));
        }
    }
}
