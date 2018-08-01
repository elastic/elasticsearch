/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.watch.Payload;
import org.elasticsearch.xpack.watcher.common.http.HttpProxy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class IncidentEventTests extends ESTestCase {

    public void testPagerDutyXContent() throws IOException {

        String serviceKey = randomAlphaOfLength(3);
        boolean attachPayload = randomBoolean();
        Payload payload = null;
        if (attachPayload) {
            payload = new Payload.Simple(Collections.singletonMap(randomAlphaOfLength(3), randomAlphaOfLength(3)));
        }
        String watchId = randomAlphaOfLength(3);
        String description = randomAlphaOfLength(3);
        String eventType = randomAlphaOfLength(3);
        String incidentKey = rarely() ? null : randomAlphaOfLength(3);
        String client = rarely() ? null : randomAlphaOfLength(3);
        String clientUrl = rarely() ? null : randomAlphaOfLength(3);
        String account = rarely() ? null : randomAlphaOfLength(3);

        IncidentEventContext[] contexts = null;
        List<IncidentEventContext> links = new ArrayList<>();
        List<IncidentEventContext> images = new ArrayList<>();

        if (randomBoolean()) {
            int numContexts = randomIntBetween(0, 3);
            contexts = new IncidentEventContext[numContexts];
            for (int i = 0; i < numContexts; i++) {
                if (randomBoolean()) {
                    contexts[i] = IncidentEventContext.link("href", "text");
                    links.add(contexts[i]);
                } else {
                    contexts[i] = IncidentEventContext.image("src", "href", "alt");
                    images.add(contexts[i]);
                }
            }
        }

        HttpProxy proxy = rarely() ? null : HttpProxy.NO_PROXY;

        IncidentEvent event = new IncidentEvent(description, eventType, incidentKey, client, clientUrl, account,
            attachPayload, contexts, proxy);

        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject(); // since its a snippet
        event.buildAPIXContent(jsonBuilder, ToXContent.EMPTY_PARAMS, serviceKey, payload, watchId);
        jsonBuilder.endObject();
        XContentParser parser = createParser(jsonBuilder);
        parser.nextToken();

        String actualServiceKey = null;
        String actualWatchId = "watcher"; // hardcoded if the SOURCE is null
        String actualDescription = null;
        String actualEventType = null;
        String actualIncidentKey = null;
        String actualClient = null;
        String actualClientUrl = null;
        String actualSeverity = null;
        List<IncidentEventContext> actualLinks = new ArrayList<>();
        List<IncidentEventContext> actualImages = new ArrayList<>();
        Payload actualPayload = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (IncidentEvent.Fields.ROUTING_KEY.match(currentFieldName, parser.getDeprecationHandler())) {
                actualServiceKey = parser.text();
            } else if (IncidentEvent.Fields.EVENT_ACTION.match(currentFieldName, parser.getDeprecationHandler())) {
                actualEventType = parser.text();
            } else if (IncidentEvent.Fields.DEDUP_KEY.match(currentFieldName, parser.getDeprecationHandler())) {
                actualIncidentKey = parser.text();
            } else if (IncidentEvent.Fields.CLIENT.match(currentFieldName, parser.getDeprecationHandler())) {
                actualClient = parser.text();
            } else if (IncidentEvent.Fields.CLIENT_URL.match(currentFieldName, parser.getDeprecationHandler())) {
                actualClientUrl = parser.text();
            } else if (IncidentEvent.Fields.LINKS.match(currentFieldName, parser.getDeprecationHandler())) {
                // this is an array
                if (token != XContentParser.Token.START_ARRAY) {
                    fail("Links was not an array");
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    actualLinks.add(IncidentEventContext.parse(parser));
                }
            } else if (IncidentEvent.Fields.IMAGES.match(currentFieldName, parser.getDeprecationHandler())) {
                // this is an array
                if (token != XContentParser.Token.START_ARRAY) {
                    fail("Images was not an array");
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    actualImages.add(IncidentEventContext.parse(parser));
                }
            } else if (IncidentEvent.Fields.PAYLOAD.match(currentFieldName, parser.getDeprecationHandler())) {
                // this is a nested object containing a few interesting bits
                //actualPayload = new Payload.Simple(parser.map());
                if (token != XContentParser.Token.START_OBJECT) {
                    fail("payload was not an object");
                }
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (IncidentEvent.Fields.SUMMARY.match(currentFieldName, parser.getDeprecationHandler())) {
                        actualDescription = parser.text();
                    } else if (IncidentEvent.Fields.SOURCE.match(currentFieldName, parser.getDeprecationHandler())) {
                        actualWatchId = parser.text();
                    } else if (IncidentEvent.Fields.SEVERITY.match(currentFieldName, parser.getDeprecationHandler())) {
                        actualSeverity = parser.text();
                    } else if (IncidentEvent.Fields.CUSTOM_DETAILS.match(currentFieldName, parser.getDeprecationHandler())) {
                        // nested payload object is in here
                        if (token != XContentParser.Token.START_OBJECT) {
                            fail("custom_details was not an object");
                        }
                        parser.nextToken();
                        Map<String, Object> mapped = parser.map();
                        // the first entry is the payload
                        actualPayload = new Payload.Simple((Map<String, Object>) mapped.get("payload"));
                    } else {
                        fail("an unexpected field was encountered inside payload: " + currentFieldName);
                    }
                }
            } else {
                // this case should not happen
                fail("an unexpected field was encountered: " + currentFieldName);
            }
        }

        // assert the actuals were the same as expected
        assertThat(serviceKey, equalTo(actualServiceKey));
        assertThat(eventType, equalTo(actualEventType));
        assertThat(incidentKey, equalTo(actualIncidentKey));
        assertThat(description, equalTo(actualDescription));
        assertThat(watchId, equalTo(actualWatchId));
        assertThat("critical", equalTo(actualSeverity));
        assertThat(client, equalTo(actualClient));
        assertThat(clientUrl, equalTo(actualClientUrl));
        assertThat(links, equalTo(actualLinks));
        assertThat(images, equalTo(actualImages));
        assertThat(payload, equalTo(actualPayload));
    }
}
