/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.marvel.MonitoredSystem;
import org.elasticsearch.marvel.agent.exporter.MonitoringDoc;
import org.joda.time.format.DateTimeFormat;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver.DELIMITER;
import static org.elasticsearch.marvel.agent.resolver.MonitoringIndexNameResolver.PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TimestampedResolverTests extends MonitoringIndexNameResolverTestCase {

    private MonitoredSystem randomId = randomFrom(MonitoredSystem.values());
    private int randomVersion = randomIntBetween(0, 100);

    @Override
    protected MonitoringIndexNameResolver<MonitoringDoc> newResolver() {
        return newTimestampedResolver(randomId, randomVersion, Settings.EMPTY);
    }

    @Override
    protected MonitoringDoc newMarvelDoc() {
        MonitoringDoc doc = new MonitoringDoc(randomMonitoringId(), randomAsciiOfLength(2));
        doc.setClusterUUID(randomAsciiOfLength(5));
        doc.setTimestamp(Math.abs(randomLong()));
        doc.setSourceNode(new DiscoveryNode(randomAsciiOfLength(5), DummyTransportAddress.INSTANCE, Version.CURRENT));
        return doc;
    }

    @Override
    protected boolean checkResolvedType() {
        return false;
    }

    @Override
    protected boolean checkResolvedId() {
        return false;
    }

    @Override
    protected boolean checkFilters() {
        return false;
    }

    public void testTimestampedResolver() {
        final MonitoringDoc doc = newMarvelDoc();
        doc.setTimestamp(1437580442979L); // "2015-07-22T15:54:02.979Z"

        for (String format : Arrays.asList("YYYY", "YYYY.MM", "YYYY.MM.dd", "YYYY.MM.dd-HH", "YYYY.MM.dd-HH.mm", "YYYY.MM.dd-HH.mm.SS")) {
            Settings settings = Settings.EMPTY;
            if (format != null) {
                settings = settingsBuilder()
                            .put(MonitoringIndexNameResolver.Timestamped.INDEX_NAME_TIME_FORMAT_SETTING.getKey(), format)
                            .build();
            }

            MonitoringIndexNameResolver.Timestamped resolver = newTimestampedResolver(randomId, randomVersion, settings);
            assertThat(resolver, notNullValue());
            assertThat(resolver.getId(), equalTo(randomId.getSystem()));
            assertThat(resolver.getVersion(), equalTo(randomVersion));
            assertThat(resolver.index(doc),
                    equalTo(PREFIX + DELIMITER + resolver.getId() + DELIMITER + String.valueOf(resolver.getVersion())
                            + DELIMITER + DateTimeFormat.forPattern(format).withZoneUTC().print(1437580442979L)));
        }
    }

    private MonitoringIndexNameResolver.Timestamped<MonitoringDoc> newTimestampedResolver(MonitoredSystem id, int version,
                                                                                          Settings settings) {
        return new MonitoringIndexNameResolver.Timestamped<MonitoringDoc>(id, version, settings) {
            @Override
            public String type(MonitoringDoc document) {
                return null;
            }

            @Override
            protected void buildXContent(MonitoringDoc document, XContentBuilder builder, ToXContent.Params params) throws IOException {
                return;
            }
        };
    }
}
