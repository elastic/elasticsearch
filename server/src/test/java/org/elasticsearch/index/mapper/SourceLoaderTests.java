/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SourceLoaderTests extends MapperServiceTestCase {
    public void testEmptyObject() throws IOException {
        DocumentMapper mapper = createDocumentMapper(syntheticSourceMapping(b -> {
            b.startObject("o").field("type", "object").endObject();
            b.startObject("kwd").field("type", "keyword").endObject();
        }));
        assertThat(syntheticSource(mapper, b -> b.field("kwd", "foo")), equalTo("""
            {"kwd":"foo"}"""));
    }

    public void testUnsupported() throws IOException {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> createDocumentMapper(syntheticSourceMapping(b -> b.startObject("txt").field("type", "text").endObject()))
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "field [txt] of type [text] doesn't support synthetic source unless"
                    + " it has a sub-field of type [keyword] with doc values enabled"
            )
        );
    }

    public void testSorted() throws IOException {
        DocumentMapper mapper = createDocumentMapper(syntheticSourceMapping(b -> {
            b.startObject("foo").field("type", "keyword").endObject();
            b.startObject("bar").field("type", "keyword").endObject();
            b.startObject("baz").field("type", "keyword").endObject();
        }));
        assertThat(
            syntheticSource(mapper, b -> b.field("foo", "over the lazy dog").field("bar", "the quick").field("baz", "brown fox jumped")),
            equalTo("""
                {"bar":"the quick","baz":"brown fox jumped","foo":"over the lazy dog"}""")
        );
    }
}
