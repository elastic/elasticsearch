/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedField;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedFields;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

class SearchHitToJsonProcessor implements Releasable {

    private final ExtractedFields fields;
    private final XContentBuilder jsonBuilder;

    SearchHitToJsonProcessor(ExtractedFields fields, OutputStream outputStream) throws IOException {
        this.fields = Objects.requireNonNull(fields);
        this.jsonBuilder = new XContentBuilder(JsonXContent.jsonXContent, outputStream);
    }

    public void process(SearchHit hit) throws IOException {
        jsonBuilder.startObject();
        for (ExtractedField field : fields.getAllFields()) {
            writeKeyValue(field.getAlias(), field.value(hit));
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
