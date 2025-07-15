/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.ingest.ESONSource;
import org.elasticsearch.plugins.internal.XContentParserDecorator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

public class ESONSourceBenchmarkTests extends ESTestCase {

    private final BytesArray source = new BytesArray(
        "{\"@timestamp\":\"2021-04-28T19:45:28.222Z\",\"kubernetes\":{\"namespace\":\"namespace0\",\"node\":{\"name\":\"gke-apps-node-name-0\"},\"pod\":{\"name\":\"pod-name-pod-name-0\"},\"volume\":{\"name\":\"volume-0\",\"fs\":{\"capacity\":{\"bytes\":7883960320},\"used\":{\"bytes\":12288},\"inodes\":{\"used\":9,\"free\":1924786,\"count\":1924795},\"available\":{\"bytes\":7883948032}}}},\"metricset\":{\"name\":\"volume\",\"period\":10000},\"fields\":{\"cluster\":\"elastic-apps\"},\"host\":{\"name\":\"gke-apps-host-name0\"},\"agent\":{\"id\":\"96db921d-d0a0-4d00-93b7-2b6cfc591bc3\",\"version\":\"7.6.2\",\"type\":\"metricbeat\",\"ephemeral_id\":\"c0aee896-0c67-45e4-ba76-68fcd6ec4cde\",\"hostname\":\"gke-apps-host-name-0\"},\"ecs\":{\"version\":\"1.4.0\"},\"service\":{\"address\":\"service-address-0\",\"type\":\"kubernetes\"},\"event\":{\"dataset\":\"kubernetes.volume\",\"module\":\"kubernetes\",\"duration\":132588484}}"
    );

    public void testMap() {
        System.err.println(source.length());
        Map<String, Object> stringObjectMap = XContentHelper.convertToMap(source, false, XContentType.JSON, XContentParserDecorator.NOOP)
            .v2();
        System.err.println(stringObjectMap);
    }

    public void testESON() throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                source.array(),
                source.arrayOffset(),
                source.length()
            )
        ) {
            ESONSource.ESONObject root = new ESONSource.Builder(false).parse(parser);
            System.err.println(root);
            XContentBuilder builder = XContentFactory.contentBuilder(JsonXContent.jsonXContent.type());
            root.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference bytes = BytesReference.bytes(builder);
            System.err.println(bytes.utf8ToString());
        }
    }
}
