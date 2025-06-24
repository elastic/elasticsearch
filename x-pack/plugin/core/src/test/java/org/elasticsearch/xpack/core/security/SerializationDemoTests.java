/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static org.elasticsearch.TransportVersions.PARTIAL_DATA_DEMO;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SerializationDemoTests extends ESTestCase {

    record SearchResult(boolean success, @Nullable List<String> results, @Nullable List<String> failures)
        implements
            Writeable,
            // ToXContentFragment also exists
            ToXContentObject {

        private static final ConstructingObjectParser<SearchResult, Void> PARSER = buildParser();

        @SuppressWarnings("unchecked")
        private static ConstructingObjectParser<SearchResult, Void> buildParser() {
            final ConstructingObjectParser<SearchResult, Void> parser = new ConstructingObjectParser<>(
                "search_result",
                true,
                a -> new SearchResult((boolean) a[0], (List<String>) a[1], (List<String>) a[2])
            );
            parser.declareBoolean(constructorArg(), new ParseField("success"));
            parser.declareStringArray(optionalConstructorArg(), new ParseField("results"));
            parser.declareStringArray(optionalConstructorArg(), new ParseField("failures"));
            return parser;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBoolean(success);
            out.writeOptionalCollection(results, StreamOutput::writeString);
            // Elasticsearch supports rolling upgrades across 1 major version and within major versions.
            // For example 7.17 needs to be able to communicate with 8.4 nodes, and 8.1 nodes need to be able to talk with 8.4 nodes.
            // Serverless removed the notion of transport versions being tied cleanly to ES versions since we release to serverless
            // every week and have rolling upgrades
            if (out.getTransportVersion().onOrAfter(PARTIAL_DATA_DEMO)) {
                out.writeOptionalCollection(failures, StreamOutput::writeString);
            }
        }

        SearchResult(StreamInput input) throws IOException {
            this(
                input.readBoolean(),
                input.readOptionalCollectionAsList(StreamInput::readString),
                input.getTransportVersion().onOrAfter(PARTIAL_DATA_DEMO)
                    ? input.readOptionalCollectionAsList(StreamInput::readString)
                    : List.of()
            );
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            var xcb = builder.startObject().field("success", success);
            if (results != null) {
                xcb = xcb.field("results", results);
            }
            if (failures != null) {
                xcb = xcb.field("failures", failures);
            }
            return xcb.endObject();
        }

        public SearchResult fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

    }

    public void testRoundTripTransportSerialization() throws IOException {
        var result = new SearchResult(true, List.of("hit1"), List.of());

        try (var out = new BytesStreamOutput()) {
            result.writeTo(out);
            var received = new SearchResult(out.bytes().streamInput());

            System.out.println("Original: " + result);
            System.out.println("Received: " + received);
        }
    }

    public void testToXContent() {
        var result = new SearchResult(true, List.of("hit1", "hit2"), List.of("failure1"));

        try (var builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
            String json = Strings.toString(builder);
            System.out.println("JSON Output: " + json);
            // test from XContent
            try (
                var parser = XContentType.JSON.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE,
                        new ByteArrayInputStream(json.getBytes())
                    )
            ) {
                var parsedResult = result.fromXContent(parser);
                System.out.println("Parsed Result: " + parsedResult);
            }
        } catch (IOException e) {
            fail("Failed to convert to XContent: " + e.getMessage());
        }
    }

}
