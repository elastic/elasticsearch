/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompositeSyntheticFieldLoaderTests extends ESTestCase {
    public void testComposingMultipleStoredFields() throws IOException {
        var sut = new CompositeSyntheticFieldLoader(
            "foo",
            "bar.baz.foo",
            List.of(new CompositeSyntheticFieldLoader.StoredFieldLayer("foo.one") {
                @Override
                protected void writeValue(Object value, XContentBuilder b) throws IOException {
                    b.value((long) value);
                }
            }, new CompositeSyntheticFieldLoader.StoredFieldLayer("foo.two") {
                @Override
                protected void writeValue(Object value, XContentBuilder b) throws IOException {
                    b.value((long) value);
                }
            })
        );

        var storedFieldLoaders = sut.storedFieldLoaders().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        storedFieldLoaders.get("foo.one").advanceToDoc(0);
        storedFieldLoaders.get("foo.one").load(List.of(45L, 46L));
        storedFieldLoaders.get("foo.two").advanceToDoc(0);
        storedFieldLoaders.get("foo.two").load(List.of(1L));

        var result = XContentBuilder.builder(XContentType.JSON.xContent());
        result.startObject();
        sut.write(result);
        result.endObject();

        assertEquals("""
            {"foo":[45,46,1]}""", Strings.toString(result));
    }

    public void testLoadStoredFieldAndAdvance() throws IOException {
        var sut = new CompositeSyntheticFieldLoader(
            "foo",
            "bar.baz.foo",
            List.of(new CompositeSyntheticFieldLoader.StoredFieldLayer("foo.one") {
                @Override
                protected void writeValue(Object value, XContentBuilder b) throws IOException {
                    b.value((long) value);
                }
            })
        );

        var storedFieldLoaders = sut.storedFieldLoaders().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        storedFieldLoaders.get("foo.one").advanceToDoc(0);
        storedFieldLoaders.get("foo.one").load(List.of(45L));

        var result = XContentBuilder.builder(XContentType.JSON.xContent());
        result.startObject();
        sut.write(result);
        result.endObject();

        assertEquals("""
            {"foo":45}""", Strings.toString(result));

        storedFieldLoaders.get("foo.one").advanceToDoc(1);

        var empty = XContentBuilder.builder(XContentType.JSON.xContent());
        empty.startObject();
        sut.write(result);
        empty.endObject();

        assertEquals("{}", Strings.toString(empty));
    }

    public void testComposingMultipleDocValuesFields() throws IOException {
        var sut = new CompositeSyntheticFieldLoader("foo", "bar.baz.foo", List.of(new CompositeSyntheticFieldLoader.Layer() {
            @Override
            public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
                return Stream.empty();
            }

            @Override
            public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
                return (docId -> true);
            }

            @Override
            public boolean hasValue() {
                return true;
            }

            @Override
            public void write(XContentBuilder b) throws IOException {
                b.value(45L);
                b.value(46L);
            }

            @Override
            public String fieldName() {
                return "";
            }

            @Override
            public long valueCount() {
                return 2;
            }
        }, new CompositeSyntheticFieldLoader.Layer() {
            @Override
            public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
                return Stream.empty();
            }

            @Override
            public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
                return (docId -> true);
            }

            @Override
            public boolean hasValue() {
                return true;
            }

            @Override
            public void write(XContentBuilder b) throws IOException {
                b.value(1L);
            }

            @Override
            public String fieldName() {
                return "";
            }

            @Override
            public long valueCount() {
                return 1;
            }
        }));

        sut.docValuesLoader(null, new int[0]).advanceToDoc(0);

        var result = XContentBuilder.builder(XContentType.JSON.xContent());
        result.startObject();
        sut.write(result);
        result.endObject();

        assertEquals("""
            {"foo":[45,46,1]}""", Strings.toString(result));
    }

    public void testComposingStoredFieldsWithDocValues() throws IOException {
        var sut = new CompositeSyntheticFieldLoader(
            "foo",
            "bar.baz.foo",
            List.of(new CompositeSyntheticFieldLoader.StoredFieldLayer("foo.one") {
                @Override
                protected void writeValue(Object value, XContentBuilder b) throws IOException {
                    b.value((long) value);
                }
            }, new CompositeSyntheticFieldLoader.Layer() {
                @Override
                public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
                    return Stream.empty();
                }

                @Override
                public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
                    return (docId -> true);
                }

                @Override
                public boolean hasValue() {
                    return true;
                }

                @Override
                public void write(XContentBuilder b) throws IOException {
                    b.value(1L);
                }

                @Override
                public String fieldName() {
                    return "";
                }

                @Override
                public long valueCount() {
                    return 1;
                }
            })
        );

        var storedFieldLoaders = sut.storedFieldLoaders().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        storedFieldLoaders.get("foo.one").advanceToDoc(0);
        storedFieldLoaders.get("foo.one").load(List.of(45L, 46L));

        sut.docValuesLoader(null, new int[0]).advanceToDoc(0);

        var result = XContentBuilder.builder(XContentType.JSON.xContent());
        result.startObject();
        sut.write(result);
        result.endObject();

        assertEquals("""
            {"foo":[45,46,1]}""", Strings.toString(result));
    }

    public void testFieldName() {
        var sut = new CompositeSyntheticFieldLoader("foo", "bar.baz.foo");
        assertEquals("bar.baz.foo", sut.fieldName());
    }
}
