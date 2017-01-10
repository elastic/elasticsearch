/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.scheduler.extractor;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.io.OutputStream;

public class SearchHitToJsonProcessor implements Releasable {

    private final String[] fields;
    private final XContentBuilder jsonBuilder;

    public SearchHitToJsonProcessor(String[] fields, OutputStream outputStream) throws IOException {
        this.fields = fields;
        jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, outputStream);
    }

    public void process(SearchHit hit) throws IOException {
        jsonBuilder.startObject();
        for (String field : fields) {
            writeKeyValue(field, SearchHitFieldExtractor.extractField(hit, field));
        }
        jsonBuilder.endObject();
    }

    private void writeKeyValue(String key, Object... values) throws IOException {
        if (values.length == 0) {
            return;
        }
        if (values.length == 1) {
            jsonBuilder.field(key, values[0]);
        } else {
            jsonBuilder.array(key, values);
        }
    }

    @Override
    public void close() {
        jsonBuilder.close();
    }
}
