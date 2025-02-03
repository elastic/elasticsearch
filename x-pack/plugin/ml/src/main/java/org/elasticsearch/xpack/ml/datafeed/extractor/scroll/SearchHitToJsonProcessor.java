/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.scroll;

import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ml.extractor.ExtractedField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.elasticsearch.xpack.ml.extractor.SourceSupplier;

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

    public void process(SearchHit hit, SourceSupplier sourceSupplier) throws IOException {
        jsonBuilder.startObject();
        for (ExtractedField field : fields.getAllFields()) {
            writeKeyValue(field.getName(), field.value(hit, sourceSupplier));
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
