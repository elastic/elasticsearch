/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.action.search.SearchCapabilities;
import org.elasticsearch.search.profile.ProfileSelection.Names;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ProfileSelectionTests extends ESTestCase {

    public void testCodecReadsAreProfiledExactlyWhenTheFeatureFlagIsEnabled() {
        final ProfileSelection requested = randomFrom(
            ProfileSelection.NONE,
            ProfileSelection.EVERYTHING,
            ProfileSelection.of(Names.EVERY, null),
            ProfileSelection.of(null, Names.of(List.of("host.name")))
        );
        final boolean enabled = ProfileSelection.CODEC_READ_PROFILING_FEATURE_FLAG.isEnabled();
        if (enabled) {
            assertThat(ProfileSelection.ifEnabled(requested), sameInstance(requested));
        } else {
            assertThat("a profiled request opens no codec read scope", ProfileSelection.ifEnabled(requested), nullValue());
        }
        assertThat(SearchCapabilities.CAPABILITIES.contains("codec_read_profiling"), equalTo(enabled));
    }

    public void testTheTotalsOnly() throws IOException {
        for (String body : List.of("{}", "{\"segments\": []}", "{\"fields\": []}", "{\"segments\": [], \"fields\": []}")) {
            final ProfileSelection selection = parse(body);
            assertThat(body, selection, equalTo(ProfileSelection.NONE));
            assertTrue(body, selection.isNone());
            assertFalse(body, selection.selectsSegment("_1", "index:0/_1"));
            assertFalse(body, selection.selectsField("host.name"));
            assertFalse(body, selection.reportsFields());
            assertFalse(body, selection.describesSegments());
            assertFalse(body, selection.describesFields());
        }
    }

    public void testNamedSegmentsReportTheirTotalsOnly() throws IOException {
        final ProfileSelection selection = parse("{\"segments\": [\"_1\", \"_3\", \"_1\"]}");
        assertThat(selection, equalTo(ProfileSelection.of(Names.of(List.of("_3", "_1")), null)));
        assertTrue(selection.selectsSegment("_1", null));
        assertTrue(selection.selectsSegment("_3", "index:0/_3"));
        assertFalse(selection.selectsSegment("_2", "index:0/_2"));
        assertFalse("segment names are matched exactly", selection.selectsSegment("_10", null));
        assertFalse("a named segment does not expand its fields", selection.breaksDown("host.name"));
        assertFalse(selection.selectsField("host.name"));
        assertFalse(selection.reportsFields());
        assertFalse("a name asks for no description", selection.describesSegments());
        assertFalse(selection.describesFields());
    }

    public void testASegmentIsNamedByItsLuceneNameOrItsKey() throws IOException {
        final ProfileSelection selection = parse("{\"segments\": [\"logs-a:0/_1\"]}");
        assertTrue(selection.selectsSegment("_1", "logs-a:0/_1"));
        assertFalse("the key names one shard only", selection.selectsSegment("_1", "logs-a:1/_1"));
        assertFalse("a key needs the shard to match", selection.selectsSegment("_1", null));
        final ProfileSelection byName = parse("{\"segments\": [\"_1\"]}");
        assertTrue("a Lucene name matches in every shard", byName.selectsSegment("_1", "logs-a:0/_1"));
        assertTrue(byName.selectsSegment("_1", "logs-b:3/_1"));
    }

    public void testNamedFieldsReportTheirTotalsOnly() throws IOException {
        final ProfileSelection selection = parse("{\"fields\": [\"host.name\", \"region\"]}");
        assertTrue(selection.selectsField("host.name"));
        assertTrue(selection.selectsField("region"));
        assertFalse(selection.selectsField("host"));
        assertFalse("names are not patterns", selection.selectsField("host.name.keyword"));
        assertFalse("a named field does not expand its segments", selection.selectsSegment("_1", "index:0/_1"));
        assertTrue(selection.reportsFields());
        assertFalse(selection.describesSegments());
        assertFalse(selection.describesFields());
    }

    public void testEverySegmentExpandsNoField() throws IOException {
        final ProfileSelection selection = parse("{\"segments\": \"_all\"}");
        assertThat(selection, equalTo(ProfileSelection.of(Names.EVERY, null)));
        assertTrue(selection.selectsSegment("_" + randomAlphaOfLength(3), null));
        assertFalse("every segment breaks down no field", selection.breaksDown(randomAlphaOfLength(5)));
        assertFalse("every segment reports no field totals", selection.reportsFields());
        assertTrue(selection.describesSegments());
        assertFalse("every segment describes no field", selection.describesFields());
    }

    public void testEveryFieldExpandsNoSegment() throws IOException {
        final ProfileSelection selection = parse("{\"fields\": \"_all\"}");
        assertTrue(selection.selectsField(randomAlphaOfLength(5)));
        assertFalse(selection.selectsSegment("_1", "index:0/_1"));
        assertTrue(selection.reportsFields());
        assertTrue(selection.describesFields());
        assertFalse("every field describes no segment", selection.describesSegments());
    }

    public void testEverySegmentAndEveryFieldIsTheCompleteBreakdown() throws IOException {
        final ProfileSelection selection = parse("{\"segments\": \"_all\", \"fields\": \"_all\"}");
        assertThat(selection, equalTo(ProfileSelection.EVERYTHING));
        assertTrue(selection.selectsSegment("_" + randomAlphaOfLength(3), null));
        assertTrue(selection.breaksDown(randomAlphaOfLength(5)));
        assertTrue(selection.reportsFields());
        assertTrue(selection.describesSegments());
        assertTrue(selection.describesFields());
    }

    public void testSegmentsAndFieldsCombine() throws IOException {
        final ProfileSelection named = parse("{\"segments\": [\"_1\"], \"fields\": [\"host.name\"]}");
        assertTrue(named.selectsSegment("_1", null));
        assertTrue(named.selectsField("host.name"));
        assertTrue("the selected fields are broken down in a selected segment", named.breaksDown("host.name"));
        assertFalse("an unselected field is not", named.breaksDown("region"));
        assertFalse(named.describesSegments());
        assertFalse(named.describesFields());

        final ProfileSelection everyField = parse("{\"segments\": [\"_1\"], \"fields\": \"_all\"}");
        assertTrue(everyField.breaksDown("region"));
        assertFalse(everyField.selectsSegment("_2", null));
        assertTrue("every field describes the fields", everyField.describesFields());
        assertFalse("a named segment is not described", everyField.describesSegments());

        final ProfileSelection everySegment = parse("{\"segments\": \"_all\", \"fields\": [\"host.name\"]}");
        assertTrue(everySegment.breaksDown("host.name"));
        assertFalse("named fields narrow the breakdown of every segment", everySegment.breaksDown("region"));
        assertTrue("every segment describes the segments", everySegment.describesSegments());
        assertFalse("a named field is not described", everySegment.describesFields());
    }

    public void testAnEntryOfAnArrayIsAlwaysAName() throws IOException {
        final ProfileSelection selection = parse("{\"segments\": [\"_all\"], \"fields\": [\"_all\"]}");
        assertTrue(selection.selectsSegment("_all", null));
        assertFalse(selection.selectsSegment("_1", null));
        assertTrue(selection.selectsField("_all"));
        assertFalse(selection.selectsField("host.name"));
        assertFalse(selection.describesSegments());
        assertFalse(selection.describesFields());
    }

    public void testCountingNestedDocsIsAskedForApart() throws IOException {
        final ProfileSelection selection = parse("{\"segments\": \"_all\", \"count_nested_docs\": true}");
        assertTrue(selection.countNestedDocs());
        assertThat(selection, equalTo(ProfileSelection.of(Names.EVERY, null).withCountNestedDocs(true)));
        assertNotEquals(selection, ProfileSelection.of(Names.EVERY, null));
        assertFalse(parse("{\"segments\": \"_all\"}").countNestedDocs());
        assertFalse(parse("{\"segments\": \"_all\", \"count_nested_docs\": false}").countNestedDocs());
        expectThrows(XContentParseException.class, () -> parse("{\"count_nested_docs\": \"yes\"}"));
    }

    public void testInvalidSelectionsAreRejected() {
        for (String body : List.of(
            "{\"segments\": \"_1\"}",
            "{\"segments\": \"*\"}",
            "{\"segments\": {}}",
            "{\"segments\": {\"_1\": {}}}",
            "{\"segments\": [1]}",
            "{\"segments\": [[\"_1\"]]}",
            "{\"segments\": true}",
            "{\"fields\": \"host.name\"}",
            "{\"fields\": \"*\"}",
            "{\"fields\": {}}",
            "{\"fields\": [1]}",
            "{\"columns\": []}"
        )) {
            final XContentParseException e = expectThrows(XContentParseException.class, () -> parse(body));
            assertNotNull(body, e.getMessage());
        }
        final XContentParseException segment = expectThrows(XContentParseException.class, () -> parse("{\"segments\": \"_1\"}"));
        assertThat(segment.getMessage(), containsString("[profile.segments] must be [_all] or an array of segment names but was [_1]"));
        final XContentParseException field = expectThrows(XContentParseException.class, () -> parse("{\"fields\": [1]}"));
        assertThat(field.getMessage(), containsString("[profile.fields] must hold field names only"));
    }

    public void testRoundTripsThroughXContentAndTheWire() throws IOException {
        final ProfileSelection selection = ProfileSelection.of(randomNames(), randomNames()).withCountNestedDocs(randomBoolean());
        final XContentBuilder builder = JsonXContent.contentBuilder();
        selection.toXContent(builder);
        assertThat(Strings.toString(builder), parse(Strings.toString(builder)), equalTo(selection));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            selection.writeTo(out);
            out.writeString("trailing");
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(ProfileSelection.readFrom(in), equalTo(selection));
                assertThat("the selection reads exactly what it wrote", in.readString(), equalTo("trailing"));
            }
        }
    }

    private static Names randomNames() {
        return switch (between(0, 2)) {
            case 0 -> null;
            case 1 -> Names.EVERY;
            case 2 -> Names.of(randomList(0, 4, () -> randomFrom("_all", "_" + randomAlphaOfLength(2), "logs:0/_1")));
            default -> throw new AssertionError();
        };
    }

    private ProfileSelection parse(String body) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, body)) {
            parser.nextToken();
            return ProfileSelection.parseProfileObject(parser);
        }
    }
}
