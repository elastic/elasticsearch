/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class IndexRecoveryInTimeSeriesModeIT extends IndexRecoveryIT {
    @Override
    protected boolean inTimeSeriesMode() {
        return true;
    }

    @Override
    protected String minimalMapping() throws IOException {
        XContentBuilder b = JsonXContent.contentBuilder().startObject().startObject("properties");
        b.startObject("@timestamp").field("type", "date").endObject();
        b.startObject("dim").field("type", "keyword").field("dimension", true).endObject();
        return Strings.toString(b.endObject().endObject());
    }

    @Override
    protected Map<String, ?> source(Map<String, ?> source) {
        Map<String, Object> compatibleSource = new LinkedHashMap<>(source);
        compatibleSource.put("@timestamp", "2022-01-01T00:00:00Z");
        compatibleSource.put("dim", "dimval");
        return compatibleSource;
    }

    @Override
    protected boolean dummyDocuments() {
        return false;
    }

    @Override
    protected BackgroundIndexer backgroundIndexer(int numOfDocs) {
        return new BackgroundIndexer(INDEX_NAME, "_doc", client(), numOfDocs) {
            @Override
            protected void extraSource(XContentBuilder builder) throws IOException {
                builder.field("@timestamp", "2022-01-01T00:00:00Z");
                builder.field("dim", "dimval");
            }
        };
    }
}
