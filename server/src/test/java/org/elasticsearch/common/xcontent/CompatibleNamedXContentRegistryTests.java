/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.IsEqual.equalTo;

/**
 * A test where a named object parser is overridden by a compatible implementation in order to support the old parser (N-1)
 * The old parser is only available when a parser is using compatibility (created when a request was compatible)
 */
public class CompatibleNamedXContentRegistryTests extends ESTestCase {
    static class ParentObject {
        private static final ConstructingObjectParser<ParentObject, String> PARSER =
            new ConstructingObjectParser<>("parentParser", false,
                (a, name) -> new ParentObject(name, (SubObject) a[0]));

        String name;
        SubObject subObject;

        static {
            PARSER.declareNamedObject(ConstructingObjectParser.constructorArg(),
                (p, c, n) -> p.namedObject(SubObject.class, n, null),
                new ParseField("subObject"));
        }

        ParentObject(String name, SubObject subObject) {
            this.name = name;
            this.subObject = subObject;
        }


        public static ParentObject parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    static class SubObject {
        private static final ConstructingObjectParser<SubObject, String> PARSER = new ConstructingObjectParser<>(
            "parser1", false,
            a -> new SubObject((String) a[0]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("new_field"));
        }

        String field;

        SubObject(String field) {
            this.field = field;
        }

        public static SubObject parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    static class OldSubObject extends SubObject {
        private static final ConstructingObjectParser<SubObject, String> PARSER = new ConstructingObjectParser<>(
            "parser2", false,
            a -> new SubObject((String) a[0]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("old_field"));
        }

        OldSubObject(String field) {
            super(field);
        }

        public static SubObject parse(XContentParser parser) {
            return PARSER.apply(parser, null);
        }
    }

    public void testNotCompatibleRequest() throws IOException {
        NamedXContentRegistry registry = new NamedXContentRegistry(
            Arrays.asList(new NamedXContentRegistry.Entry(SubObject.class, new ParseField("namedObjectName1"), SubObject::parse)),
            Arrays.asList(new NamedXContentRegistry.Entry(SubObject.class, new ParseField("namedObjectName1"), OldSubObject::parse)));

        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());
        b.startObject();
        b.startObject("subObject");
        b.startObject("namedObjectName1");
        b.field("new_field", "value1");
        b.endObject();
        b.endObject();
        b.endObject();

        String mediaType = XContentType.VND_JSON.toParsedMediaType()
            .responseContentTypeHeader(Map.of(MediaType.COMPATIBLE_WITH_PARAMETER_NAME,
                String.valueOf(Version.CURRENT.major)));
        List<String> mediaTypeList = Collections.singletonList(mediaType);

        RestRequest restRequest = new FakeRestRequest.Builder(registry)
            .withContent(BytesReference.bytes(b), RestRequest.parseContentType(mediaTypeList))
            .withPath("/foo")
            .withHeaders(Map.of("Content-Type", mediaTypeList, "Accept", mediaTypeList))
            .build();

        try (XContentParser p = restRequest.contentParser()) {
            ParentObject parse = ParentObject.parse(p);
            assertThat(parse.subObject.field, equalTo("value1"));
        }

    }

    public void testCompatibleRequest() throws IOException {
        NamedXContentRegistry compatibleRegistry = new NamedXContentRegistry(
            Arrays.asList(new NamedXContentRegistry.Entry(SubObject.class, new ParseField("namedObjectName1"), SubObject::parse)),
            Arrays.asList(new NamedXContentRegistry.Entry(SubObject.class, new ParseField("namedObjectName1"), OldSubObject::parse)));

        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());
        b.startObject();
        b.startObject("subObject");
        b.startObject("namedObjectName1");
        b.field("old_field", "value1");
        b.endObject();
        b.endObject();
        b.endObject();
        String mediaType = XContentType.VND_JSON.toParsedMediaType()
            .responseContentTypeHeader(Map.of(MediaType.COMPATIBLE_WITH_PARAMETER_NAME,
                String.valueOf(RestApiVersion.minimumSupported().major)));
        List<String> mediaTypeList = Collections.singletonList(mediaType);

        RestRequest restRequest2 = new FakeRestRequest.Builder(compatibleRegistry)
            .withContent(BytesReference.bytes(b), RestRequest.parseContentType(mediaTypeList))
            .withPath("/foo")
            .withHeaders(Map.of("Content-Type", mediaTypeList, "Accept", mediaTypeList))
            .build();

        try (XContentParser p = restRequest2.contentParser()) {
            ParentObject parse = ParentObject.parse(p);
            assertThat(parse.subObject.field, equalTo("value1"));
        }
    }
}
