/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.action.suggestions.CursorMarker;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class EsqlSuggestionsRequestTests extends ESTestCase {

    public void testDefaults() {
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest();
        assertEquals(EsqlSuggestionsRequest.DEFAULT_SIZE, request.size());
        assertFalse(request.includeSampleValues());
    }

    public void testValidateAcceptsCursorAtBounds() {
        String query = "FROM foo | KEEP a";
        assertThat(new EsqlSuggestionsRequest().query(query).cursor(0).validate(), nullValue());
        assertThat(new EsqlSuggestionsRequest().query(query).cursor(query.length()).validate(), nullValue());
    }

    public void testValidateRejectsMissingQuery() {
        ActionRequestValidationException e = new EsqlSuggestionsRequest().cursor(0).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[query] is required"));
    }

    public void testValidateRejectsOutOfRangeCursor() {
        ActionRequestValidationException e = new EsqlSuggestionsRequest().query("FROM foo").cursor(999).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[cursor] must be within"));
    }

    public void testValidateRejectsNonPositiveSize() {
        ActionRequestValidationException e = new EsqlSuggestionsRequest().query("FROM foo").cursor(0).size(0).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[size] must be greater than 0"));
    }

    public void testSerializationRoundTrip() throws Exception {
        // Cursor sits right before the 'x' literal, matching the position used pre-marker.
        CursorMarker marker = CursorMarker.of("FROM foo | WHERE a == \"<*>x\"");
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query(marker.query())
            .cursor(marker.cursor())
            .size(25)
            .includeSampleValues(true);
        assertRoundTrip(request);
    }

    public void testSerializationRoundTripWithMultilineQuery() throws Exception {
        // Confirms serialization doesn't mangle an embedded real newline in the query text.
        CursorMarker marker = CursorMarker.of("FROM foo\n| WHERE a == \"<*>x\"\n| KEEP a");
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query(marker.query())
            .cursor(marker.cursor())
            .size(25)
            .includeSampleValues(true);
        assertRoundTrip(request);
    }

    private void assertRoundTrip(EsqlSuggestionsRequest request) throws Exception {
        EsqlSuggestionsRequest read;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                read = new EsqlSuggestionsRequest(in);
            }
        }
        assertEquals(request.query(), read.query());
        assertEquals(request.cursor(), read.cursor());
        assertEquals(request.size(), read.size());
        assertEquals(request.includeSampleValues(), read.includeSampleValues());
    }

    public void testIsCompositeIndicesRequestMarkerOnly() {
        // A static text parse of `query()` cannot see through view/dataset resolution (see the suggestions API
        // spec), so this request declares no indices() of its own; real per-index authorization happens later,
        // in dataset/field-caps resolution and the hot-tier sampling path's own FLS/DLS gate.
        EsqlSuggestionsRequest request = new EsqlSuggestionsRequest().query("FROM foo | KEEP a");
        assertThat(request, instanceOf(CompositeIndicesRequest.class));
    }
}
