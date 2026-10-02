/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.ql.InvalidArgumentException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * A timestamp that is indexed but absent from the stored {@code _source} must fail a sequence with a client error.
 * Not a YAML REST test: Serverless reuses those suites and rejects the {@code _source} mapping parameters used here.
 */
public class SequenceTimestampMissingFromSourceIT extends AbstractEqlIntegTestCase {

    public void testSequenceOverTimestampWithDisabledSource() {
        assertSequenceFailsOnMissingTimestamp("""
            { "enabled": false }""");
    }

    public void testSequenceOverTimestampExcludedFromSource() {
        assertSequenceFailsOnMissingTimestamp("""
            { "excludes": [ "@timestamp" ] }""");
    }

    public void testSequenceOverTimestampNotIncludedInSource() {
        assertSequenceFailsOnMissingTimestamp("""
            { "includes": [ "value" ] }""");
    }

    private void assertSequenceFailsOnMissingTimestamp(String sourceMapping) {
        assertAcked(indicesAdmin().prepareCreate("test").setMapping(Strings.format("""
            {
              "_source": %s,
              "properties": { "@timestamp": { "type": "date" }, "value": { "type": "long" } }
            }""", sourceMapping)));
        prepareIndex("test").setSource("@timestamp", "2020-12-03T11:04:05.000Z", "value", 1).get();
        refresh("test");

        EqlSearchRequest request = new EqlSearchRequest().indices("test").query("sequence [any where true] [any where true]");
        InvalidArgumentException e = expectThrows(
            InvalidArgumentException.class,
            () -> client().execute(EqlSearchAction.INSTANCE, request).actionGet()
        );

        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            e.getMessage(),
            equalTo(
                "Expected timestamp field [@timestamp] to have a value but got none; "
                    + "check that [_source] is enabled and includes the field"
            )
        );
    }
}
