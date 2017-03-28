/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.bulk;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.monitoring.action.MonitoringBulkDoc;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver;

import java.io.IOException;

public class MonitoringBulkDataResolver extends MonitoringIndexNameResolver.Data<MonitoringBulkDoc> {

    @Override
    protected void buildXContent(MonitoringBulkDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
        BytesReference source = document.getSource();
        if (source != null && source.length() > 0) {
            builder.rawField(document.getType(), source, document.getXContentType());
        }
    }
}