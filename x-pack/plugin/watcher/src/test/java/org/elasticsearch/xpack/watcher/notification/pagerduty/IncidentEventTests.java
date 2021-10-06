/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.notification.pagerduty;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;
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

        ObjectPath objectPath = ObjectPath.createFromXContent(jsonBuilder.contentType().xContent(), BytesReference.bytes(jsonBuilder));

        String actualServiceKey = objectPath.evaluate(IncidentEvent.Fields.ROUTING_KEY.getPreferredName());
        String actualWatchId = objectPath.evaluate(IncidentEvent.Fields.PAYLOAD.getPreferredName()
            + "." + IncidentEvent.Fields.SOURCE.getPreferredName());
        if (actualWatchId == null) {
            actualWatchId = "watcher"; // hardcoded if the SOURCE is null
        }
        String actualDescription = objectPath.evaluate(IncidentEvent.Fields.PAYLOAD.getPreferredName()
            + "." + IncidentEvent.Fields.SUMMARY.getPreferredName());
        String actualEventType = objectPath.evaluate(IncidentEvent.Fields.EVENT_ACTION.getPreferredName());
        String actualIncidentKey = objectPath.evaluate(IncidentEvent.Fields.DEDUP_KEY.getPreferredName());
        String actualClient = objectPath.evaluate(IncidentEvent.Fields.CLIENT.getPreferredName());
        String actualClientUrl = objectPath.evaluate(IncidentEvent.Fields.CLIENT_URL.getPreferredName());
        String actualSeverity = objectPath.evaluate(IncidentEvent.Fields.PAYLOAD.getPreferredName()
            + "." + IncidentEvent.Fields.SEVERITY.getPreferredName());
        Map<String, Object> payloadDetails = objectPath.evaluate("payload.custom_details.payload");
        Payload actualPayload = null;

        if (payloadDetails != null) {
            actualPayload = new Payload.Simple(payloadDetails);
        }

        List<IncidentEventContext> actualLinks = new ArrayList<>();
        List<Map<String, String>> linkMap = objectPath.evaluate(IncidentEvent.Fields.LINKS.getPreferredName());
        if (linkMap != null) {
            for (Map<String, String> iecValue : linkMap) {
                actualLinks.add(IncidentEventContext.link(iecValue.get("href"), iecValue.get("text")));
            }
        }

        List<IncidentEventContext> actualImages = new ArrayList<>();
        List<Map<String, String>> imgMap = objectPath.evaluate(IncidentEvent.Fields.IMAGES.getPreferredName());
        if (imgMap != null) {
            for (Map<String, String> iecValue : imgMap) {
                actualImages.add(IncidentEventContext.image(iecValue.get("src"), iecValue.get("href"), iecValue.get("alt")));
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
