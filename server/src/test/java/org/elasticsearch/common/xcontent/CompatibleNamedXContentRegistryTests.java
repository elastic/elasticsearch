/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.MediaType;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.hamcrest.core.IsEqual.equalTo;

/**
 * A test where a named object parser is overridden by a compatible implementation in order to support the old parser (N-1)
 * The old parser is only available when a parser is using compatibility (created when a request was compatible)
 */
public class CompatibleNamedXContentRegistryTests extends ESTestCase {
    static class ParentObject {
        private static final ConstructingObjectParser<ParentObject, String> PARSER = new ConstructingObjectParser<>(
            "parentParser",
            false,
            (a, name) -> new ParentObject(name, (NewSubObject) a[0])
        );

        String name;
        NewSubObject subObject;

        static {
            PARSER.declareNamedObject(
                ConstructingObjectParser.constructorArg(),
                (p, c, n) -> p.namedObject(NewSubObject.class, n, null),
                new ParseField("subObject")
            );
        }

        ParentObject(String name, NewSubObject subObject) {
            this.name = name;
            this.subObject = subObject;
        }

        public static ParentObject parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    static class NewSubObject {
        public static final Predicate<RestApiVersion> REST_API_VERSION = RestApiVersion.onOrAfter(RestApiVersion.current());
        private static final ConstructingObjectParser<NewSubObject, String> PARSER = new ConstructingObjectParser<>(
            "parser1",
            false,
            a -> new NewSubObject((String) a[0])
        );
        static final ParseField NAME = new ParseField("namedObjectName1").forRestApiVersion(
            RestApiVersion.onOrAfter(RestApiVersion.current())
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("new_field"));
        }

        String field;

        NewSubObject(String field) {
            this.field = field;
        }

        public static NewSubObject parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    static class OldSubObject {
        public static final Predicate<RestApiVersion> REST_API_VERSION = RestApiVersion.equalTo(RestApiVersion.minimumSupported());

        private static final ConstructingObjectParser<NewSubObject, String> PARSER = new ConstructingObjectParser<>(
            "parser2",
            false,
            a -> new NewSubObject((String) a[0])
        );
        static final ParseField NAME = new ParseField("namedObjectName1").forRestApiVersion(
            RestApiVersion.equalTo(RestApiVersion.minimumSupported())
        );
        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("old_field"));
        }
        String field;

        OldSubObject(String field) {
            this.field = field;
        }

        public static NewSubObject parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    public void testNotCompatibleRequest() throws IOException {
        NamedXContentRegistry registry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(NewSubObject.class, NewSubObject.NAME, NewSubObject::parse, NewSubObject.REST_API_VERSION),
                new NamedXContentRegistry.Entry(NewSubObject.class, OldSubObject.NAME, OldSubObject::parse, OldSubObject.REST_API_VERSION)
            )
        );

        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());
        b.startObject();
        b.startObject("subObject");
        b.startObject("namedObjectName1");
        b.field("new_field", "value1");
        b.endObject();
        b.endObject();
        b.endObject();

        String mediaType = XContentType.VND_JSON.toParsedMediaType()
            .responseContentTypeHeader(Map.of(MediaType.COMPATIBLE_WITH_PARAMETER_NAME, String.valueOf(Version.CURRENT.major)));
        List<String> mediaTypeList = Collections.singletonList(mediaType);

        RestRequest restRequest = new FakeRestRequest.Builder(registry).withContent(
            BytesReference.bytes(b),
            RestRequest.parseContentType(mediaTypeList)
        ).withPath("/foo").withHeaders(Map.of("Content-Type", mediaTypeList, "Accept", mediaTypeList)).build();

        try (XContentParser p = restRequest.contentParser()) {
            ParentObject parse = ParentObject.parse(p);
            assertThat(parse.subObject.field, equalTo("value1"));
        }
    }

    public void testCompatibleRequest() throws IOException {
        NamedXContentRegistry registry = new NamedXContentRegistry(
            List.of(
                new NamedXContentRegistry.Entry(NewSubObject.class, NewSubObject.NAME, NewSubObject::parse, NewSubObject.REST_API_VERSION),
                new NamedXContentRegistry.Entry(NewSubObject.class, OldSubObject.NAME, OldSubObject::parse, OldSubObject.REST_API_VERSION)
            )
        );

        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());
        b.startObject();
        b.startObject("subObject");
        b.startObject("namedObjectName1");
        b.field("old_field", "value1");
        b.endObject();
        b.endObject();
        b.endObject();
        String mediaType = XContentType.VND_JSON.toParsedMediaType()
            .responseContentTypeHeader(
                Map.of(MediaType.COMPATIBLE_WITH_PARAMETER_NAME, String.valueOf(RestApiVersion.minimumSupported().major))
            );
        List<String> mediaTypeList = Collections.singletonList(mediaType);

        RestRequest restRequest2 = new FakeRestRequest.Builder(registry).withContent(
            BytesReference.bytes(b),
            RestRequest.parseContentType(mediaTypeList)
        ).withPath("/foo").withHeaders(Map.of("Content-Type", mediaTypeList, "Accept", mediaTypeList)).build();

        try (XContentParser p = restRequest2.contentParser()) {
            ParentObject parse = ParentObject.parse(p);
            assertThat(parse.subObject.field, equalTo("value1"));
        }
    }
}
