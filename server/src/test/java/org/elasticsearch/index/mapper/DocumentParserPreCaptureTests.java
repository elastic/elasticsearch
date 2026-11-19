/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

/**
 * Pre-capture to {@code _ignored_source} is resolved per leaf value and per array while a document is parsed. When ignored source
 * cannot be written the answer is always "no", so the resolution, and the copy_to destination lookups and full path building it
 * needs, must be skipped. Lookups are observed through {@link DocumentParserContext#isCopyToDestinationField}, which only the
 * pre-capture resolution calls for the documents used here.
 */
public class DocumentParserPreCaptureTests extends MapperServiceTestCase {

    private static final String MAPPING = """
        {
          "_doc": {
            "properties": {
              "src": { "type": "keyword", "copy_to": "dest" },
              "dest": { "type": "keyword" },
              "number": { "type": "long" },
              "tags": { "type": "keyword" }
            }
          }
        }""";

    private static final String SCALARS = """
        { "src": "a", "dest": "b", "number": 1, "tags": "x" }""";

    private static final String ARRAYS = """
        { "src": ["a", "b"], "dest": ["c", "d"], "number": [1, 2], "tags": ["x", "y"] }""";

    public void testStoredSourceScalarFieldsDoNotResolvePreCapture() throws IOException {
        // given an index with stored _source
        MappingLookup mappingLookup = mappingLookup(SourceFieldMapper.Mode.STORED);

        // when a document with scalar leaf fields is parsed
        int lookups = parseCountingCopyToLookups(mappingLookup, Settings.EMPTY, SCALARS);

        // then no copy_to destination lookup is made for pre-capture
        assertEquals(0, lookups);
    }

    public void testStoredSourceArraysDoNotResolvePreCapture() throws IOException {
        // given an index with stored _source
        MappingLookup mappingLookup = mappingLookup(SourceFieldMapper.Mode.STORED);

        // when a document with value arrays is parsed
        int lookups = parseCountingCopyToLookups(mappingLookup, Settings.EMPTY, ARRAYS);

        // then no copy_to destination lookup is made, so no array full path is needed either
        assertEquals(0, lookups);
    }

    public void testSkippedIgnoredSourceWritesDoNotResolvePreCapture() throws IOException {
        // given synthetic _source with ignored source writes skipped
        MappingLookup mappingLookup = mappingLookup(SourceFieldMapper.Mode.SYNTHETIC);
        Settings settings = Settings.builder().put(IgnoredSourceFieldMapper.SKIP_IGNORED_SOURCE_WRITE_SETTING.getKey(), true).build();

        // when a document with scalars or arrays is parsed
        int lookups = parseCountingCopyToLookups(mappingLookup, settings, randomFrom(SCALARS, ARRAYS));

        // then no copy_to destination lookup is made
        assertEquals(0, lookups);
    }

    public void testWritableIgnoredSourceStillResolvesPreCapture() throws IOException {
        // given synthetic _source where ignored source can be written
        MappingLookup mappingLookup = mappingLookup(SourceFieldMapper.Mode.SYNTHETIC);

        // when a document with scalars or arrays is parsed
        int lookups = parseCountingCopyToLookups(mappingLookup, Settings.EMPTY, randomFrom(SCALARS, ARRAYS));

        // then pre-capture is still resolved, which looks up copy_to destinations
        assertTrue("expected copy_to destination lookups but got " + lookups, lookups > 0);
    }

    private MappingLookup mappingLookup(SourceFieldMapper.Mode sourceMode) throws IOException {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_MAPPER_SOURCE_MODE_SETTING.getKey(), sourceMode).build();
        return createMapperService(settings, MAPPING).mappingLookup();
    }

    /**
     * Parses {@code json} at the root of {@code mappingLookup} and returns how often copy_to destinations were looked up.
     * A {@link TestDocumentParserContext} subclass is used because the lookup is otherwise unobservable: it only costs time.
     */
    private int parseCountingCopyToLookups(MappingLookup mappingLookup, Settings settings, String json) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            int[] lookups = new int[1];
            DocumentParserContext context = new TestDocumentParserContext(mappingLookup, null, settings) {
                @Override
                public XContentParser parser() {
                    return parser;
                }

                @Override
                public boolean isCopyToDestinationField(String name) {
                    lookups[0]++;
                    return super.isCopyToDestinationField(name);
                }
            };
            parser.nextToken();
            DocumentParser.parseObjectOrNested(context);
            return lookups[0];
        }
    }
}
