/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.support.xcontent.WatcherParams;
import org.elasticsearch.xpack.watcher.common.http.HttpRequest;
import org.elasticsearch.xpack.watcher.common.http.HttpResponse;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class SentEventTests extends ESTestCase {

    public void testToXContentBodyFiltering() throws Exception {
        HttpResponse response = new HttpResponse(500);
        String body = randomAlphaOfLength(20);
        HttpRequest request = HttpRequest.builder("localhost", 1234).body(body).build();
        IncidentEvent incidentEvent = new IncidentEvent("description", "eventtype", null, null, null, null, false, null, null);
        SentEvent sentEvent = SentEvent.responded(incidentEvent, request, response);

        try (XContentBuilder builder = jsonBuilder()) {
            WatcherParams params = WatcherParams.builder().hideSecrets(false).build();
            sentEvent.toXContent(builder, params);
            assertThat(Strings.toString(builder), containsString(body));

            try (XContentParser parser = builder.contentType().xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            Strings.toString(builder))) {
                parser.map();
            }
        }
        try (XContentBuilder builder = jsonBuilder()) {
            sentEvent.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertThat(Strings.toString(builder), not(containsString(body)));

            try (XContentParser parser = builder.contentType().xContent()
                    .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                            Strings.toString(builder))) {
                parser.map();
            }
        }

    }

}
