/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.monitoring.MonitoredSystem;
import org.elasticsearch.xpack.monitoring.exporter.MonitoringDoc;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.monitoring.exporter.MonitoringTemplateUtils.TEMPLATE_VERSION;
import static org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver.DELIMITER;
import static org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver.Fields.CLUSTER_UUID;
import static org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver.Fields.SOURCE_NODE;
import static org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver.Fields.TIMESTAMP;
import static org.elasticsearch.xpack.monitoring.resolver.MonitoringIndexNameResolver.PREFIX;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public abstract class MonitoringIndexNameResolverTestCase<M extends MonitoringDoc, R extends MonitoringIndexNameResolver<M>>
        extends ESTestCase {

    protected final ResolversRegistry resolversRegistry = new ResolversRegistry(Settings.EMPTY);

    /**
     * @return the {@link MonitoringIndexNameResolver} to test
     */
    protected R newResolver() {
        return newResolver(newMonitoringDoc());
    }

    /**
     * @return the {@link MonitoringIndexNameResolver} to test
     */
    @SuppressWarnings("unchecked")
    protected R newResolver(M doc) {
        return (R) resolversRegistry.getResolver(doc);
    }

    /**
     * @return a new randomly created {@link MonitoringDoc} instance to use in tests
     */
    protected abstract M newMonitoringDoc();

    public void testMonitoringDoc() {
        MonitoringDoc doc = newMonitoringDoc();
        assertThat(doc.getMonitoringId(), equalTo(MonitoredSystem.fromSystem(doc.getMonitoringId()).getSystem()));
        assertThat(doc.getMonitoringVersion(), not(isEmptyOrNullString()));
        assertThat(doc.getClusterUUID(), not(isEmptyOrNullString()));
        assertThat(doc.getTimestamp(), greaterThan(0L));
    }

    protected boolean checkResolvedIndex() {
        return true;
    }

    @SuppressWarnings("unchecked")
    public void testIndex() {
        if (checkResolvedIndex()) {
            assertThat(newResolver().index(newMonitoringDoc()), not(isEmptyOrNullString()));
        } else {
            assertThat(newResolver().index(newMonitoringDoc()), is(nullValue()));
        }
    }

    protected boolean checkFilters() {
        return true;
    }

    public void testFilters() {
        if (checkFilters()) {
            assertThat(newResolver().filters(), allOf(not(empty()), notNullValue()));
        } else {
            assertThat(newResolver().filters(), anyOf(empty(), nullValue()));
        }
    }

    @SuppressWarnings("unchecked")
    public void testSource() throws IOException {
        MonitoringIndexNameResolver resolver = newResolver();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference source = resolver.source(newMonitoringDoc(), xContentType);

        assertNotNull(source);
        assertThat(source.length(), greaterThan(0));
        assertSource(source, resolver.filters(), xContentType);
    }

    @SuppressWarnings("unchecked")
    public void testResolver() {
        MonitoringIndexNameResolver resolver = newResolver();
        assertThat(resolver, notNullValue());

        if (resolver instanceof MonitoringIndexNameResolver.Timestamped) {
            MonitoringIndexNameResolver.Timestamped timestamped = (MonitoringIndexNameResolver.Timestamped) resolver;
            assertThat(resolver.index(newMonitoringDoc()),
                    startsWith(PREFIX + DELIMITER + timestamped.getId() + DELIMITER + TEMPLATE_VERSION + DELIMITER));
        }

        if (resolver instanceof MonitoringIndexNameResolver.Data) {
            assertThat(resolver.index(newMonitoringDoc()),
                    equalTo(PREFIX + DELIMITER + MonitoringIndexNameResolver.Data.DATA + DELIMITER + TEMPLATE_VERSION));
        }
    }

    protected void assertSource(BytesReference source, Set<String> fields, XContentType xContentType) {
        Map<String, Object> sourceFields = XContentHelper.convertToMap(source, false, xContentType).v2();
        assertNotNull(sourceFields);

        String[] commons = new String[]{
                CLUSTER_UUID,
                TIMESTAMP,
                SOURCE_NODE,
        };
        assertThat("source must contains default fields", sourceFields.keySet(), hasItems(commons));

        if (fields != null && fields.isEmpty() == false) {
            for (String field : fields) {
                assertSourceField(field, sourceFields);
            }

            // Checks that no extra fields are present
            Set<String> extra = new HashSet<>();
            for (String key : flattenMapKeys(new HashSet<>(), null, sourceFields)) {
                boolean found = false;
                for (String field : fields) {
                    if (key.equalsIgnoreCase(field) || key.startsWith(field + ".")) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    extra.add(key);
                }
            }
            assertThat("found extra field(s) " + extra, extra.size(), equalTo(0));
        }
    }

    protected void assertSourceField(String field, Map<String, Object> sourceFields) {
        assertNotNull("source must contain field [" + field + "] with a non-null value",
                XContentMapValues.extractValue(field, sourceFields));
    }

    protected static String randomMonitoringId() {
        return randomFrom(MonitoredSystem.values()).getSystem();
    }

    @SuppressWarnings("unchecked")
    private static Set<String> flattenMapKeys(final Set<String> keys, String path, Map<String, Object> map) {
        if (map == null || map.isEmpty()) {
            return keys;
        }

        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String p = path != null ? path + "." + entry.getKey() : entry.getKey();
            if (entry.getValue() instanceof Map) {
                flattenMapKeys(keys, p, (Map)entry.getValue());
            } else {
                keys.add(p);
            }
        }
        return keys;
    }
}
