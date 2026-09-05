/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompositeSyntheticFieldLoaderTests extends MapperServiceTestCase {

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
        storedFieldLoaders.get("foo.one").load(List.of(45L, 46L));
        storedFieldLoaders.get("foo.two").load(List.of(1L));

        var result = XContentBuilder.builder(XContentType.JSON.xContent());
        result.startObject();
        sut.write(result);
        result.endObject();

        assertEquals("""
            {"foo":[45,46,1]}""", Strings.toString(result));
    }

    public void testLoadStoredFieldAndReset() throws IOException {
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
        storedFieldLoaders.get("foo.one").load(List.of(45L));

        var result = XContentBuilder.builder(XContentType.JSON.xContent());
        result.startObject();
        sut.write(result);
        result.endObject();

        assertEquals("""
            {"foo":45}""", Strings.toString(result));

        var empty = XContentBuilder.builder(XContentType.JSON.xContent());
        empty.startObject();
        // reset() should have been called after previous write
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
            public void reset() {

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
            public void reset() {

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
                public void reset() {

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

    public void testAddFallbackLayersUsesIgnoreMalformedColumnForPreMergeStrictColumnarIndex() throws IOException {
        var layers = fallbackLayers(IndexVersions.COLUMNAR_DOC_VALUES_CODEC_FEATURE_FLAG, /* strictColumnar */ true, true, false);

        assertEquals("expected exactly one fallback layer for pre-merge strict-columnar index", 1, layers.size());
        assertEquals(
            "pre-merge strict-columnar index must read malformed values from ._ignore_malformed, not ._on_failure",
            IgnoreMalformedStoredValues.name("field"),
            layers.get(0).fieldName()
        );
    }

    public void testAddFallbackLayersUsesOnFailureColumnForCurrentVersionStrictColumnarIndex() throws IOException {
        var layers = fallbackLayers(IndexVersion.current(), /* strictColumnar */ true, true, false);

        assertEquals("expected exactly one fallback layer for current strict-columnar index", 1, layers.size());
        assertEquals(
            "current strict-columnar index must read malformed values from ._on_failure",
            OnFailureStoredValues.name("field"),
            layers.get(0).fieldName()
        );
    }

    /**
     * When both {@code ignore_malformed=true} and {@code doc_values.on_failure=ignore} are set on the same field in a
     * strict-columnar index, the write path routes malformed values to {@code ._on_failure} (not {@code ._ignore_malformed}),
     * so both constraints share a single sidecar column. The read path must therefore add exactly one on-failure layer —
     * adding both would cause every value to be emitted twice.
     */
    public void testAddFallbackLayersBothFlagsOnInStrictColumnarAddsExactlyOneLayer() throws IOException {
        assumeTrue("doc_values on_failure feature flag must be enabled", FieldMapper.DOC_VALUES_ON_FAILURE_FEATURE_FLAG.isEnabled());
        var layers = fallbackLayers(IndexVersion.current(), /* strictColumnar */ true, true, true);

        assertEquals("ignoreMalformed+onFailureEnabled in strict-columnar must add exactly one layer", 1, layers.size());
        assertEquals("the single layer must be ._on_failure", OnFailureStoredValues.name("field"), layers.get(0).fieldName());
    }

    /**
     * Builds an integer field mapper with the given {@code ignore_malformed} and {@code doc_values.on_failure=ignore} flags, in
     * either a strict-columnar or a standard index, then returns the list of fallback layers that
     * {@link CompositeSyntheticFieldLoader#addFallbackLayers(List, FieldMapper, IndexSettings)} would append.
     */
    private List<CompositeSyntheticFieldLoader.Layer> fallbackLayers(
        IndexVersion version,
        boolean strictColumnar,
        boolean ignoreMalformed,
        boolean onFailureEnabled
    ) throws IOException {
        Settings settings = strictColumnar ? Settings.builder().put(IndexSettings.MODE.getKey(), "columnar").build() : Settings.EMPTY;
        var mapperService = createMapperService(version, settings, fieldMapping(b -> {
            b.field("type", "integer");
            if (ignoreMalformed) {
                b.field("ignore_malformed", true);
            }
            if (onFailureEnabled) {
                b.startObject("doc_values").field("on_failure", "ignore").endObject();
            }
        }));
        var mapper = (FieldMapper) mapperService.mappingLookup().getMapper("field");
        var layers = new ArrayList<CompositeSyntheticFieldLoader.Layer>();
        CompositeSyntheticFieldLoader.addFallbackLayers(layers, mapper, mapperService.getIndexSettings());
        return layers;
    }

    public void testMergeTwoFieldLoaders() throws IOException {
        // given
        var fieldLoader1 = new CompositeSyntheticFieldLoader(
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

        var fieldLoader2 = new CompositeSyntheticFieldLoader(
            "foo",
            "bar.baz.foo",
            List.of(new CompositeSyntheticFieldLoader.StoredFieldLayer("foo.three") {
                @Override
                protected void writeValue(Object value, XContentBuilder b) throws IOException {
                    b.value((long) value);
                }
            })
        );

        var mergedFieldLoader = fieldLoader1.mergedWith(fieldLoader2);

        var storedFieldLoaders = mergedFieldLoader.storedFieldLoaders().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        storedFieldLoaders.get("foo.one").load(List.of(45L, 46L));
        storedFieldLoaders.get("foo.two").load(List.of(1L));
        storedFieldLoaders.get("foo.three").load(List.of(98L, 99L));

        // when
        var result = XContentBuilder.builder(XContentType.JSON.xContent());
        result.startObject();
        mergedFieldLoader.write(result);
        result.endObject();

        // then
        assertEquals("""
            {"foo":[45,46,1,98,99]}""", Strings.toString(result));
    }
}
