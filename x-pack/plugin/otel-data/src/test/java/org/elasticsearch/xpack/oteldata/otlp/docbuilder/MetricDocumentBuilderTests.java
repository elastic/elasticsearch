/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.oteldata.otlp.docbuilder;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.oteldata.otlp.datapoint.DataPointGroupingContext;
import org.elasticsearch.xpack.oteldata.otlp.proto.BufferedByteStringAccessor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MetricDocumentBuilderTests extends MetricDocumentTestCase {

    @Override
    protected boolean producesTsid() {
        return true;
    }

    @Override
    protected Document buildDocument(MappingHints defaultMappingHints, DataPointGroupingContext.DataPointGroup dataPointGroup)
        throws IOException {
        Map<String, String> dynamicTemplates = new HashMap<>();
        Map<String, Map<String, String>> dynamicTemplateParams = new HashMap<>();
        MetricDocumentBuilder documentBuilder = new MetricDocumentBuilder(new BufferedByteStringAccessor(), defaultMappingHints);
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        BytesRef tsid = documentBuilder.buildMetricDocument(builder, dataPointGroup, dynamicTemplates, dynamicTemplateParams, tsidLayout);
        ObjectPath doc = ObjectPath.createFromXContent(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        return new Document(doc, dynamicTemplates, dynamicTemplateParams, tsid);
    }
}
