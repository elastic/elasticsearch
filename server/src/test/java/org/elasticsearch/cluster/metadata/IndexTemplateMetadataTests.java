/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class IndexTemplateMetadataTests extends ESTestCase {

    public void testIndexTemplateMetadataXContentRoundTrip() throws Exception {

        String template = Strings.format("""
            {
              "index_patterns": [ ".test-*" ],
              "order": 1000,
              "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0
              },
              "mappings": {
                "doc": {
                  "properties": {
                    "%s": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""", randomAlphaOfLength(10), randomAlphaOfLength(10));

        BytesReference templateBytes = new BytesArray(template);
        final IndexTemplateMetadata indexTemplateMetadata;
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                templateBytes,
                XContentType.JSON
            )
        ) {
            indexTemplateMetadata = IndexTemplateMetadata.Builder.fromXContent(parser, "test");
        }

        final BytesReference templateBytesRoundTrip;
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.startObject();
            IndexTemplateMetadata.Builder.toXContentWithTypes(indexTemplateMetadata, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            templateBytesRoundTrip = BytesReference.bytes(builder);
        }

        final IndexTemplateMetadata indexTemplateMetadataRoundTrip;
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                templateBytesRoundTrip,
                XContentType.JSON
            )
        ) {
            indexTemplateMetadataRoundTrip = IndexTemplateMetadata.Builder.fromXContent(parser, "test");
        }
        assertThat(indexTemplateMetadata, equalTo(indexTemplateMetadataRoundTrip));
    }

    public void testValidateInvalidIndexPatterns() throws Exception {
        final IllegalArgumentException emptyPatternError = expectThrows(IllegalArgumentException.class, () -> {
            new IndexTemplateMetadata(
                randomRealisticUnicodeOfLengthBetween(5, 10),
                randomInt(),
                randomInt(),
                Collections.emptyList(),
                Settings.EMPTY,
                Map.of(),
                Map.of()
            );
        });
        assertThat(emptyPatternError.getMessage(), equalTo("Index patterns must not be null or empty; got []"));

        final IllegalArgumentException nullPatternError = expectThrows(IllegalArgumentException.class, () -> {
            new IndexTemplateMetadata(
                randomRealisticUnicodeOfLengthBetween(5, 10),
                randomInt(),
                randomInt(),
                null,
                Settings.EMPTY,
                Map.of(),
                Map.of()
            );
        });
        assertThat(nullPatternError.getMessage(), equalTo("Index patterns must not be null or empty; got null"));

        final String templateWithEmptyPattern = Strings.format("""
            {
              "index_patterns": [],
              "order": 1000,
              "settings": {
                "number_of_shards": 10,
                "number_of_replicas": 1
              },
              "mappings": {
                "doc": {
                  "properties": {
                    "%s": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""", randomAlphaOfLength(10), randomAlphaOfLength(10));
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray(templateWithEmptyPattern),
                XContentType.JSON
            )
        ) {
            final IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> IndexTemplateMetadata.Builder.fromXContent(parser, randomAlphaOfLengthBetween(1, 100))
            );
            assertThat(ex.getMessage(), equalTo("Index patterns must not be null or empty; got []"));
        }

        final String templateWithoutPattern = Strings.format("""
            {
              "order": 1000,
              "settings": {
                "number_of_shards": 10,
                "number_of_replicas": 1
              },
              "mappings": {
                "doc": {
                  "properties": {
                    "%s": {
                      "type": "text"
                    },
                    "%s": {
                      "type": "keyword"
                    }
                  }
                }
              }
            }""", randomAlphaOfLength(10), randomAlphaOfLength(10));
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray(templateWithoutPattern),
                XContentType.JSON
            )
        ) {
            final IllegalArgumentException ex = expectThrows(
                IllegalArgumentException.class,
                () -> IndexTemplateMetadata.Builder.fromXContent(parser, randomAlphaOfLengthBetween(1, 100))
            );
            assertThat(ex.getMessage(), equalTo("Index patterns must not be null or empty; got null"));
        }
    }

    public void testParseTemplateWithAliases() throws Exception {
        String templateInJSON = """
            {"aliases": {"log":{}}, "index_patterns": ["pattern-1"]}""";
        try (
            XContentParser parser = XContentHelper.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                new BytesArray(templateInJSON),
                XContentType.JSON
            )
        ) {
            IndexTemplateMetadata template = IndexTemplateMetadata.Builder.fromXContent(parser, randomAlphaOfLengthBetween(1, 100));
            assertThat(template.aliases().containsKey("log"), equalTo(true));
            assertThat(template.patterns(), contains("pattern-1"));
        }
    }

    public void testFromToXContent() throws Exception {
        String templateName = randomUnicodeOfCodepointLengthBetween(1, 10);
        IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder(templateName);
        templateBuilder.patterns(Arrays.asList("pattern-1"));
        int numAlias = between(0, 5);
        for (int i = 0; i < numAlias; i++) {
            AliasMetadata.Builder alias = AliasMetadata.builder(randomRealisticUnicodeOfLengthBetween(1, 100));
            if (randomBoolean()) {
                alias.indexRouting(randomRealisticUnicodeOfLengthBetween(1, 100));
            }
            if (randomBoolean()) {
                alias.searchRouting(randomRealisticUnicodeOfLengthBetween(1, 100));
            }
            templateBuilder.putAlias(alias);
        }
        if (randomBoolean()) {
            templateBuilder.settings(Settings.builder().put("index.setting-1", randomLong()));
            templateBuilder.settings(Settings.builder().put("index.setting-2", randomTimeValue()));
        }
        if (randomBoolean()) {
            templateBuilder.order(randomInt());
        }
        if (randomBoolean()) {
            templateBuilder.version(between(0, 100));
        }
        if (randomBoolean()) {
            templateBuilder.putMapping("doc", """
                {"doc":{"properties":{"type":"text"}}}""");
        }
        IndexTemplateMetadata template = templateBuilder.build();
        XContentBuilder builder = XContentBuilder.builder(randomFrom(XContentType.JSON.xContent()));
        builder.startObject();
        IndexTemplateMetadata.Builder.toXContentWithTypes(template, builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(shuffleXContent(builder))) {
            IndexTemplateMetadata parsed = IndexTemplateMetadata.Builder.fromXContent(parser, templateName);
            assertThat(parsed, equalTo(template));
        }
    }
}
