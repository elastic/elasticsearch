/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.deprecation;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.deprecation.DeprecationIssue.Level;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.core.IsEqual.equalTo;

public class DeprecationIssueTests extends ESTestCase {

    private DeprecationIssue issue;

    static DeprecationIssue createTestInstance() {
        String details = randomBoolean() ? randomAlphaOfLength(10) : null;
        return new DeprecationIssue(
            randomFrom(Level.values()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            details,
            randomBoolean(),
            randomMap(1, 5, () -> Tuple.tuple(randomAlphaOfLength(4), randomAlphaOfLength(4)))
        );
    }

    @Before
    public void setup() {
        issue = createTestInstance();
    }

    public void testEqualsAndHashCode() {
        DeprecationIssue other = new DeprecationIssue(
            issue.getLevel(),
            issue.getMessage(),
            issue.getUrl(),
            issue.getDetails(),
            issue.isResolveDuringRollingUpgrade(),
            issue.getMeta()
        );
        assertThat(issue, equalTo(other));
        assertThat(other, equalTo(issue));
        assertThat(issue.hashCode(), equalTo(other.hashCode()));
    }

    public void testSerialization() throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        issue.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        DeprecationIssue other = new DeprecationIssue(in);
        assertThat(issue, equalTo(other));
    }

    public void testToXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        issue.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> toXContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        String level = (String) toXContentMap.get("level");
        String message = (String) toXContentMap.get("message");
        String url = (String) toXContentMap.get("url");
        if (issue.getDetails() != null) {
            assertTrue(toXContentMap.containsKey("details"));
        }
        String details = (String) toXContentMap.get("details");
        boolean requiresRestart = (boolean) toXContentMap.get("resolve_during_rolling_upgrade");
        @SuppressWarnings("unchecked")
        Map<String, Object> meta = (Map<String, Object>) toXContentMap.get("_meta");
        DeprecationIssue other = new DeprecationIssue(Level.fromString(level), message, url, details, requiresRestart, meta);
        assertThat(issue, equalTo(other));
    }

    public void testGetIntersectionOfRemovableSettings() {
        assertNull(DeprecationIssue.getIntersectionOfRemovableSettings(null));
        assertNull(DeprecationIssue.getIntersectionOfRemovableSettings(Collections.emptyList()));
        Map<String, Object> randomMeta = randomMap(1, 5, () -> Tuple.tuple(randomAlphaOfLength(4), randomAlphaOfLength(4)));
        DeprecationIssue issue1 = createTestDeprecationIssue(getTestMetaMap(randomMeta, "setting.1", "setting.2", "setting.3"));
        assertEquals(issue1, DeprecationIssue.getIntersectionOfRemovableSettings(Collections.singletonList(issue1)));
        DeprecationIssue issue2 = createTestDeprecationIssue(issue1, getTestMetaMap(randomMeta, "setting.2"));
        assertNotEquals(issue1, issue2);
        assertEquals(issue2, DeprecationIssue.getIntersectionOfRemovableSettings(Arrays.asList(issue1, issue2)));
        DeprecationIssue issue3 = createTestDeprecationIssue(issue1, getTestMetaMap(randomMeta, "setting.2", "setting.4"));
        assertEquals(issue2, DeprecationIssue.getIntersectionOfRemovableSettings(Arrays.asList(issue1, issue2, issue3)));
        DeprecationIssue issue4 = createTestDeprecationIssue(issue1, getTestMetaMap(randomMeta, "setting.5"));
        DeprecationIssue emptySettingsIssue = createTestDeprecationIssue(issue1, getTestMetaMap(randomMeta));
        assertEquals(
            emptySettingsIssue,
            DeprecationIssue.getIntersectionOfRemovableSettings(Arrays.asList(issue1, issue2, issue3, issue4))
        );
        DeprecationIssue issue5 = createTestDeprecationIssue(getTestMetaMap(randomMeta, "setting.1", "setting.2", "setting.3"));
        assertEquals(issue1, DeprecationIssue.getIntersectionOfRemovableSettings(Arrays.asList(issue1, issue5)));
    }

    private static Map<String, Object> getTestMetaMap(Map<String, Object> baseMap, String... settings) {
        Map<String, Object> metaMap = new HashMap<>();
        Map<String, Object> settingsMetaMap = DeprecationIssue.createMetaMapForRemovableSettings(
            settings.length == 0 ? null : Arrays.asList(settings)
        );
        metaMap.putAll(settingsMetaMap);
        metaMap.putAll(baseMap);
        return metaMap;
    }

    private static DeprecationIssue createTestDeprecationIssue(Map<String, Object> metaMap) {
        String details = randomBoolean() ? randomAlphaOfLength(10) : null;
        return new DeprecationIssue(
            randomFrom(DeprecationIssue.Level.values()),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            details,
            randomBoolean(),
            metaMap
        );
    }

    private static DeprecationIssue createTestDeprecationIssue(DeprecationIssue seedIssue, Map<String, Object> metaMap) {
        return new DeprecationIssue(
            seedIssue.getLevel(),
            seedIssue.getMessage(),
            seedIssue.getUrl(),
            seedIssue.getDetails(),
            seedIssue.isResolveDuringRollingUpgrade(),
            metaMap
        );
    }
}
