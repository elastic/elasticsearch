/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class SourceLoaderTests extends MapperServiceTestCase {
    public void testNonSynthetic() throws IOException {
        DocumentMapper mapper = createDocumentMapper(mapping(b -> {
            b.startObject("o").field("type", "object").endObject();
            b.startObject("kwd").field("type", "keyword").endObject();
        }));
        assertFalse(mapper.mappers().newSourceLoader(null, SourceFieldMetrics.NOOP).reordersFieldValues());
    }

    public void testEmptyObject() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("o").field("type", "object").endObject();
            b.startObject("kwd").field("type", "keyword").endObject();
        })).documentMapper();
        assertTrue(mapper.mappers().newSourceLoader(null, SourceFieldMetrics.NOOP).reordersFieldValues());
        assertThat(syntheticSource(mapper, b -> b.field("kwd", "foo")), equalTo("""
            {"kwd":"foo"}"""));
    }

    public void testDotsInFieldName() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(
            mapping(b -> b.startObject("foo.bar.baz").field("type", "keyword").endObject())
        ).documentMapper();
        assertThat(syntheticSource(mapper, b -> b.field("foo.bar.baz", "aaa")), equalTo("""
            {"foo":{"bar":{"baz":"aaa"}}}"""));
    }

    public void testNoSubobjectsIntermediateObject() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("foo");
            {
                b.field("type", "object").field("subobjects", false);
                b.startObject("properties");
                {
                    b.startObject("bar.baz").field("type", "keyword").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })).documentMapper();
        assertThat(syntheticSource(mapper, b -> b.field("foo.bar.baz", "aaa")), equalTo("""
            {"foo":{"bar.baz":"aaa"}}"""));
    }

    public void testNoSubobjectsRootObject() throws IOException {
        XContentBuilder mappings = topMapping(b -> {
            b.field("subobjects", false);
            b.startObject("properties");
            b.startObject("foo.bar.baz").field("type", "keyword").endObject();
            b.endObject();
        });
        DocumentMapper mapper = createSytheticSourceMapperService(mappings).documentMapper();
        assertThat(syntheticSource(mapper, b -> b.field("foo.bar.baz", "aaa")), equalTo("""
            {"foo.bar.baz":"aaa"}"""));
    }

    public void testSorted() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("foo").field("type", "keyword").endObject();
            b.startObject("bar").field("type", "keyword").endObject();
            b.startObject("baz").field("type", "keyword").endObject();
        })).documentMapper();
        assertThat(
            syntheticSource(mapper, b -> b.field("foo", "over the lazy dog").field("bar", "the quick").field("baz", "brown fox jumped")),
            equalTo("""
                {"bar":"the quick","baz":"brown fox jumped","foo":"over the lazy dog"}""")
        );
    }

    public void testArraysPushedToLeaves() throws IOException {
        DocumentMapper mapper = createSytheticSourceMapperService(mapping(b -> {
            b.startObject("o").startObject("properties");
            b.startObject("foo").field("type", "keyword").endObject();
            b.startObject("bar").field("type", "keyword").endObject();
            b.endObject().endObject();
        })).documentMapper();
        assertThat(syntheticSource(mapper, b -> {
            b.startArray("o");
            b.startObject().field("foo", "a").endObject();
            b.startObject().field("bar", "b").endObject();
            b.startObject().field("bar", "c").field("foo", "d").endObject();
            b.startObject().startArray("bar").value("e").value("f").endArray().endObject();
            b.endArray();
        }), equalTo("""
            {"o":{"bar":["b","c","e","f"],"foo":["a","d"]}}"""));
    }

    public void testHideTheCopyTo() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> createSytheticSourceMapperService(mapping(b -> {
            b.startObject("foo");
            {
                b.field("type", "keyword");
                b.startObject("fields");
                {
                    b.startObject("hidden").field("type", "keyword").field("copy_to", "bar").endObject();
                }
                b.endObject();
            }
            b.endObject();
        })));
        assertThat(e.getMessage(), equalTo("[copy_to] may not be used to copy from a multi-field: [foo.hidden]"));
    }
}
