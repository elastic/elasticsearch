/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.lucene.tests.util.TestUtil.randomSimpleString;
import static org.elasticsearch.index.reindex.AbstractBulkIndexByPaginatedSearchRequest.MAX_DESCRIPTION_PARAMS_LENGTH;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class UpdateByQueryRequestTests extends AbstractBulkByPaginatedSearchRequestTestCase<UpdateByQueryRequest> {
    public void testUpdateByQueryRequestImplementsIndicesRequestReplaceable() {
        int numIndices = between(1, 100);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomSimpleString(random(), 1, 30);
        }

        IndicesOptions indicesOptions = IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean());

        UpdateByQueryRequest request = new UpdateByQueryRequest();
        request.indices(indices);
        request.setIndicesOptions(indicesOptions);
        for (int i = 0; i < numIndices; i++) {
            assertEquals(indices[i], request.indices()[i]);
        }

        assertSame(indicesOptions, request.indicesOptions());
        assertSame(request.indicesOptions(), request.getSearchRequest().indicesOptions());

        int numNewIndices = between(1, 100);
        String[] newIndices = new String[numNewIndices];
        for (int i = 0; i < numNewIndices; i++) {
            newIndices[i] = randomSimpleString(random(), 1, 30);
        }
        request.indices(newIndices);
        for (int i = 0; i < numNewIndices; i++) {
            assertEquals(newIndices[i], request.indices()[i]);
        }
        for (int i = 0; i < numNewIndices; i++) {
            assertEquals(newIndices[i], request.getSearchRequest().indices()[i]);
        }
    }

    public void testValidateGivenRemoteIndex() {
        SearchRequest searchRequest = new SearchRequest();
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(searchRequest);
        updateByQueryRequest.indices("remote:index");
        searchRequest.source().query(QueryBuilders.matchAllQuery());

        ActionRequestValidationException e = updateByQueryRequest.validate();

        assertThat(e, is(not(nullValue())));
        assertThat(
            e.getMessage(),
            containsString("Cross-cluster calls are not supported in this context but remote indices were requested")
        );
    }

    /** Params that fit in the limit are rendered exactly as {@link Script#toString()} renders them. */
    public void testDescriptionIncludesSmallScriptParamsInFull() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("name", "value");
        params.put("count", 3);
        params.put("flag", true);
        params.put("nothing", null);
        params.put("list", List.of("a", 1, Map.of("k", "v")));
        params.put("nested", Map.of("empty", List.of()));
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "ctx._source.a = params.name", params);

        UpdateByQueryRequest request = new UpdateByQueryRequest("index").setScript(script);

        assertThat(request.getDescription(), equalTo("update-by-query [index] updated with " + script));
    }

    /** A value that is iterable but not a collection, such as a path, is rendered with its own {@link Object#toString()}. */
    public void testDescriptionRendersNonCollectionIterableParamsAsToString() {
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "return", Map.of("path", PathUtils.get("a", "b")));

        UpdateByQueryRequest request = new UpdateByQueryRequest("index").setScript(script);

        assertThat(request.getDescription(), equalTo("update-by-query [index] updated with " + script));
    }

    /** Large params, such as whole documents keyed by id, must not be copied into the task description. */
    public void testDescriptionTruncatesLargeNestedScriptParams() {
        Map<String, Object> docs = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            // not Latin-1, so that a full copy would be stored as UTF-16
            docs.put("doc-" + i, Map.of("message", "€".repeat(MAX_DESCRIPTION_PARAMS_LENGTH * 10), "tags", List.of("a", "b")));
        }
        Script script = new Script(
            ScriptType.INLINE,
            Script.DEFAULT_SCRIPT_LANG,
            "ctx._source = params.docs[ctx._id]",
            Map.of("docs", docs)
        );

        String description = new UpdateByQueryRequest("index").setScript(script).getDescription();

        assertThat(
            description,
            startsWith("update-by-query [index] updated with Script{type=inline, lang='painless', idOrCode='ctx._source")
        );
        assertThat(renderedParams(description), startsWith("{docs={doc-"));
        assertThat(renderedParams(description).length(), equalTo(MAX_DESCRIPTION_PARAMS_LENGTH + "...}".length()));
        assertThat(description, endsWith("...}"));
    }

    /** A single large value is cut instead of being copied whole. */
    public void testDescriptionTruncatesLargeScriptParamValue() {
        Script script = new Script(
            ScriptType.INLINE,
            Script.DEFAULT_SCRIPT_LANG,
            "ctx._source.message = params.message",
            Map.of("message", "x".repeat(MAX_DESCRIPTION_PARAMS_LENGTH * 10))
        );

        String description = new UpdateByQueryRequest("index").setScript(script).getDescription();

        assertThat(
            renderedParams(description),
            equalTo("{message=" + "x".repeat(MAX_DESCRIPTION_PARAMS_LENGTH - "{message=".length()) + "...}")
        );
    }

    /** Many small params that add up past the limit are cut wherever the limit falls, including inside a key, value or separator. */
    public void testDescriptionTruncatesManySmallScriptParams() {
        Map<String, Object> params = new LinkedHashMap<>();
        for (int i = 0; i < 100; i++) {
            params.put(randomAlphaOfLength(between(1, 10)) + i, List.of(i, "value" + i));
        }
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "return", params);

        String description = new UpdateByQueryRequest("index").setScript(script).getDescription();

        assertThat(renderedParams(description), equalTo(params.toString().substring(0, MAX_DESCRIPTION_PARAMS_LENGTH) + "...}"));
    }

    /** Params that are exactly as long as the limit are kept in full, without the truncation marker. */
    public void testDescriptionDoesNotMarkParamsThatExactlyFit() {
        String message = "x".repeat(MAX_DESCRIPTION_PARAMS_LENGTH - "{message=}".length());

        String description = descriptionWithMessageParam(message);

        // the closing brace of the script follows the params
        assertThat(renderedParams(description), equalTo("{message=" + message + "}}"));
    }

    /** The limit counts the brackets, so params are marked as cut even when only their closing brace does not fit. */
    public void testDescriptionMarksParamsThatDoNotFit() {
        String message = "x".repeat(MAX_DESCRIPTION_PARAMS_LENGTH - "{message=}".length() + 1);

        String description = descriptionWithMessageParam(message);

        assertThat(renderedParams(description), equalTo("{message=" + message + "...}"));
    }

    /** A cut must not leave the first half of a surrogate pair at the end of the params. */
    public void testDescriptionDoesNotSplitSurrogatePairs() {
        int roomForValue = MAX_DESCRIPTION_PARAMS_LENGTH - "{message=".length();
        String emoji = Character.toString(0x1F600);
        // the high surrogate of the emoji would be the last character that fits
        String message = "x".repeat(roomForValue - 1) + emoji + "tail";

        String description = descriptionWithMessageParam(message);

        assertThat(renderedParams(description), equalTo("{message=" + "x".repeat(roomForValue - 1) + "...}"));
    }

    private static String descriptionWithMessageParam(String message) {
        Script script = new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, "return", Map.of("message", message));
        return new UpdateByQueryRequest("index").setScript(script).getDescription();
    }

    /** The part of the description after {@code params=}, which is where the script params are rendered. */
    private static String renderedParams(String description) {
        String marker = "params=";
        return description.substring(description.indexOf(marker) + marker.length());
    }

    @Override
    protected UpdateByQueryRequest newRequest() {
        return new UpdateByQueryRequest(randomAlphaOfLength(5));
    }

    @Override
    protected void extraRandomizationForSlice(UpdateByQueryRequest original) {
        if (randomBoolean()) {
            original.setScript(mockScript(randomAlphaOfLength(5)));
        }
        if (randomBoolean()) {
            original.setPipeline(randomAlphaOfLength(5));
        }
    }

    @Override
    protected void extraForSliceAssertions(UpdateByQueryRequest original, UpdateByQueryRequest forSliced) {
        assertEquals(original.getScript(), forSliced.getScript());
        assertEquals(original.getPipeline(), forSliced.getPipeline());
    }

    // TODO: Implement standard to/from x-content parsing tests

    @Override
    protected UpdateByQueryRequest createTestInstance() {
        return newRequest();
    }

    @Override
    protected UpdateByQueryRequest doParseInstance(XContentParser parser) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != null) {
        }
        return newRequest();
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected void assertEqualInstances(UpdateByQueryRequest expectedInstance, UpdateByQueryRequest newInstance) {}
}
