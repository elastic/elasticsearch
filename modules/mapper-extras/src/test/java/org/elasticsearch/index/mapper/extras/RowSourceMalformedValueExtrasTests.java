/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.extras;

import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.mapper.BytesSource;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentSource;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.RowSource;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.sourcebatch.SourceRowXContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

/**
 * Parses the same documents through {@code DocumentParser} from a {@link BytesSource}, which is read with a JSON parser, and from a
 * {@link RowSource}, which is read with {@link SourceRowXContentParser}, and requires the same outcome from both, for the mappers in this
 * module that read a number from the parser they are handed without checking the token first.
 *
 * <p>The array form of a field is split into its elements, and the mapper is handed each in turn, an object included, on its start token.
 * A number accessor read there must throw, as the JSON parser's does, and not return the number that the previous element left behind.
 * With assertions on, the old behavior shows up as an assertion in {@code DocumentParser}, because the object was not consumed. With them
 * off it was silent: {@code [1.5, {"x": 1}]} was accepted and indexed five values, the stale 1.5 three times and the 1 from {@code x}
 * twice.
 */
public class RowSourceMalformedValueExtrasTests extends MapperServiceTestCase {

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return singletonList(new MapperExtrasPlugin());
    }

    /** What parsing a document came to: the fields it indexed, or the error it failed with. */
    private record Outcome(List<String> indexed, String error) {}

    private DocumentMapper documentMapper() throws IOException {
        return createDocumentMapper(mapping(b -> {
            b.startObject("scaled").field("type", "scaled_float").field("scaling_factor", 100).endObject();
            b.startObject("rank").field("type", "rank_feature").endObject();
        }));
    }

    public void testScaledFloatIsGivenAnObjectAfterANumber() throws IOException {
        final DocumentMapper mapper = documentMapper();
        assertSameOutcome(mapper, "{\"scaled\": [1.5, 2.5]}", false);
        assertSameOutcome(mapper, "{\"scaled\": [1.5, {\"x\": 1}]}", true);
        // A nested array is flattened into the field's values, not rejected.
        assertSameOutcome(mapper, "{\"scaled\": [1.5, [2.5]]}", false);
    }

    public void testRankFeatureIsGivenAnObjectAfterANumber() throws IOException {
        final DocumentMapper mapper = documentMapper();
        assertSameOutcome(mapper, "{\"rank\": 1.5}", false);
        assertSameOutcome(mapper, "{\"rank\": [1.5, {\"x\": 1}]}", true);
        assertSameOutcome(mapper, "{\"rank\": {\"x\": 1}}", true);
    }

    private void assertSameOutcome(DocumentMapper mapper, String json, boolean expectRejected) throws IOException {
        final Outcome fromJson = parse(mapper, new BytesSource(new BytesArray(json), XContentType.JSON, true));
        assertThat("rejected, from JSON: " + json, fromJson.error() != null, equalTo(expectRejected));
        try (EscfBatch batch = EscfEncoder.encode(List.of(new BytesArray(json)), XContentType.JSON)) {
            final RowSource row = new RowSource(SourceRowXContentParser.buildSchemaTree(batch.schema()), batch.row(0), XContentType.JSON);
            assertThat("the row source of " + json, parse(mapper, row), equalTo(fromJson));
        }
    }

    private static Outcome parse(DocumentMapper mapper, DocumentSource source) {
        try {
            final ParsedDocument doc = mapper.parse(
                new SourceToParse("1", source, null, Map.of(), Map.of(), XContentMeteringParserDecorator.NOOP, null)
            );
            final List<String> indexed = new ArrayList<>();
            for (IndexableField field : doc.rootDoc().getFields()) {
                if (field.name().startsWith("_") == false) {
                    indexed.add(field.name() + "=" + valueOf(field));
                }
            }
            Collections.sort(indexed);
            return new Outcome(indexed, null);
        } catch (Exception e) {
            // Only the first line, without its position: the JSON parser's message goes on to quote the source, which a row has none of.
            final String message = e.getMessage();
            final int end = message.indexOf('\n');
            final String firstLine = (end < 0 ? message : message.substring(0, end)).replaceAll("\\[\\d+:\\d+\\] ", "");
            return new Outcome(null, e.getClass().getSimpleName() + ": " + firstLine);
        }
    }

    private static Object valueOf(IndexableField field) {
        if (field.numericValue() != null) {
            return field.numericValue();
        }
        if (field.stringValue() != null) {
            return field.stringValue();
        }
        return field.binaryValue();
    }
}
