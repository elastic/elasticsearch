/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.qa;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.time.FormatNames;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;

/**
 * Challenge test dueling index.mode=logsdb with subobjects:false (baseline) against
 * index.mode=logsdb_columnar (contender). logsdb_columnar does not accept the subobjects
 * mapping parameter. The baseline uses flat dot-notation field names via subobjects:false;
 * the contender is the logsdb_columnar equivalent. Results of common operations (queries,
 * aggregations, ES|QL, field caps) must match.
 */
public class StaticLogsDbSubobjectsFalseVersusLogsDbColumnarRestIT extends BulkChallengeRestIT {

    @Override
    public void baselineSettings(Settings.Builder builder) {
        builder.put("index.mode", "logsdb");
        // In columnar disable_sequence_numbers default to true
        builder.put("index.disable_sequence_numbers", true);
    }

    @Override
    public void contenderSettings(Settings.Builder builder) {
        builder.put("index.mode", "logsdb_columnar");
    }

    @Override
    public void baselineMappings(XContentBuilder builder) throws IOException {
        builder.startObject()
            // In columnar subobjects are not allowed
            .field("subobjects", false)
            // In columnar _id is stored as doc values by default
            .startObject("_id")
            .field("mode", "columnar")
            .endObject()
            // In columnar _routing is stored as doc values by default
            .startObject("_routing")
            .field("doc_values", true)
            .endObject()
            // The contender data stream matches the built-in "logs" index template (logs-*-*),
            // which maps dynamic string fields as keyword. The baseline data stream does not
            // match that pattern, so its dynamic strings would default to text+keyword. This
            // template aligns the baseline's dynamic string mapping with the contender's.
            .startArray("dynamic_templates")
            .startObject()
            .startObject("strings_as_keyword")
            .field("match_mapping_type", "string")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endArray()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("host.name")
            .field("type", "keyword")
            .field("ignore_above", randomIntBetween(1000, 1200))
            .endObject()
            .startObject("message")
            .field("type", "keyword")
            .field("ignore_above", randomIntBetween(1000, 1200))
            .endObject()
            .startObject("method")
            .field("type", "keyword")
            .field("ignore_above", randomIntBetween(1000, 1200))
            .endObject()
            .startObject("memory_usage_bytes")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject();
    }

    @Override
    public void contenderMappings(XContentBuilder builder) throws IOException {
        // logsdb_columnar does not accept the subobjects mapping parameter.
        builder.startObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject("host.name")
            .field("type", "keyword")
            .field("ignore_above", randomIntBetween(1000, 1200))
            .endObject()
            .startObject("message")
            .field("type", "keyword")
            .field("ignore_above", randomIntBetween(1000, 1200))
            .endObject()
            .startObject("method")
            .field("type", "keyword")
            .field("ignore_above", randomIntBetween(1000, 1200))
            .endObject()
            .startObject("memory_usage_bytes")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject();
    }

    @Override
    protected boolean autoGenerateId() {
        return false;
    }

    @Override
    protected XContentBuilder generateDocument(final Instant timestamp) throws IOException {
        XContentBuilder doc = XContentFactory.jsonBuilder()
            .startObject()
            .field("@timestamp", DateFormatter.forPattern(FormatNames.STRICT_DATE_OPTIONAL_TIME.getName()).format(timestamp))
            .field("host.name", randomFrom("foo", "bar", "baz"))
            .field("message", randomFrom("a message", "another message", "still another message", "one more message"))
            .field("method", randomFrom("put", "post", "get"))
            .field("memory_usage_bytes", randomLongBetween(1000, 2000));

        // Extra fields absent from the static mapping, dynamically mapped as flat leaf fields on
        // both sides. Notation is randomized between dot-notation and JSON objects to exercise both
        // paths through DocumentParser#parseObjectDynamic() with subobjects disabled.
        boolean hasPodName = randomBoolean();
        boolean hasNamespace = randomBoolean();
        if (hasPodName || hasNamespace) {
            if (randomBoolean()) {
                if (hasPodName) doc.field("kubernetes.pod.name", randomAlphaOfLength(8));
                if (hasNamespace) doc.field("kubernetes.namespace", randomAlphaOfLength(5));
            } else {
                doc.startObject("kubernetes");
                if (hasPodName) doc.startObject("pod").field("name", randomAlphaOfLength(8)).endObject();
                if (hasNamespace) doc.field("namespace", randomAlphaOfLength(5));
                doc.endObject();
            }
        }
        if (randomBoolean()) {
            if (randomBoolean()) {
                doc.field("http.response.status_code", randomIntBetween(200, 503));
            } else {
                doc.startObject("http").startObject("response").field("status_code", randomIntBetween(200, 503)).endObject().endObject();
            }
        }

        return doc.endObject();
    }
}
