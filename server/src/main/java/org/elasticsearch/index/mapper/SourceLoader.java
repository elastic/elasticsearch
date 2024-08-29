/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.fieldvisitor.LeafStoredFieldLoader;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Loads source {@code _source} during a GET or {@code _search}.
 */
public interface SourceLoader {
    /**
     * Does this {@link SourceLoader} reorder field values?
     */
    boolean reordersFieldValues();

    /**
     * Build the loader for some segment.
     */
    Leaf leaf(LeafReader reader, int[] docIdsInLeaf) throws IOException;

    /**
     * Stream containing all non-{@code _source} stored fields required
     * to build the {@code _source}.
     */
    Set<String> requiredStoredFields();

    /**
     * Loads {@code _source} from some segment.
     */
    interface Leaf {
        /**
         * Load the {@code _source} for a document.
         * @param storedFields a loader for stored fields
         * @param docId the doc to load
         */
        Source source(LeafStoredFieldLoader storedFields, int docId) throws IOException;

        /**
         * Write the {@code _source} for a document in the provided {@link XContentBuilder}.
         * @param storedFields a loader for stored fields
         * @param docId the doc to load
         * @param b the builder to write the xcontent
         */
        void write(LeafStoredFieldLoader storedFields, int docId, XContentBuilder b) throws IOException;
    }

    /**
     * Load {@code _source} from a stored field.
     */
    SourceLoader FROM_STORED_SOURCE = new SourceLoader() {
        @Override
        public boolean reordersFieldValues() {
            return false;
        }

        @Override
        public Leaf leaf(LeafReader reader, int[] docIdsInLeaf) {
            return new Leaf() {
                @Override
                public Source source(LeafStoredFieldLoader storedFields, int docId) throws IOException {
                    return Source.fromBytes(storedFields.source());
                }

                @Override
                public void write(LeafStoredFieldLoader storedFields, int docId, XContentBuilder builder) throws IOException {
                    Source source = source(storedFields, docId);
                    builder.rawValue(source.internalSourceRef().streamInput(), source.sourceContentType());
                }
            };
        }

        @Override
        public Set<String> requiredStoredFields() {
            return Set.of();
        }
    };

    /**
     * Reconstructs {@code _source} from doc values anf stored fields.
     */
    class Synthetic implements SourceLoader {
        private final Supplier<SyntheticFieldLoader> syntheticFieldLoaderLeafSupplier;
        private final Set<String> requiredStoredFields;
        private final SourceFieldMetrics metrics;

        /**
         * Creates a {@link SourceLoader} to reconstruct {@code _source} from doc values anf stored fields.
         * @param fieldLoaderSupplier A supplier to create {@link SyntheticFieldLoader}, one for each leaf.
         * @param metrics Metrics for profiling.
         */
        public Synthetic(Supplier<SyntheticFieldLoader> fieldLoaderSupplier, SourceFieldMetrics metrics) {
            this.syntheticFieldLoaderLeafSupplier = fieldLoaderSupplier;
            this.requiredStoredFields = syntheticFieldLoaderLeafSupplier.get()
                .storedFieldLoaders()
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
            this.requiredStoredFields.add(IgnoredSourceFieldMapper.NAME);
            this.metrics = metrics;
        }

        @Override
        public boolean reordersFieldValues() {
            return true;
        }

        @Override
        public Set<String> requiredStoredFields() {
            return requiredStoredFields;
        }

        @Override
        public Leaf leaf(LeafReader reader, int[] docIdsInLeaf) throws IOException {
            SyntheticFieldLoader loader = syntheticFieldLoaderLeafSupplier.get();
            return new LeafWithMetrics(new SyntheticLeaf(loader, loader.docValuesLoader(reader, docIdsInLeaf)), metrics);
        }

        private record LeafWithMetrics(Leaf leaf, SourceFieldMetrics metrics) implements Leaf {

            @Override
            public Source source(LeafStoredFieldLoader storedFields, int docId) throws IOException {
                long startTime = metrics.getRelativeTimeSupplier().getAsLong();

                var source = leaf.source(storedFields, docId);

                TimeValue duration = TimeValue.timeValueMillis(metrics.getRelativeTimeSupplier().getAsLong() - startTime);
                metrics.recordSyntheticSourceLoadLatency(duration);

                return source;
            }

            @Override
            public void write(LeafStoredFieldLoader storedFields, int docId, XContentBuilder b) throws IOException {
                long startTime = metrics.getRelativeTimeSupplier().getAsLong();

                leaf.write(storedFields, docId, b);

                TimeValue duration = TimeValue.timeValueMillis(metrics.getRelativeTimeSupplier().getAsLong() - startTime);
                metrics.recordSyntheticSourceLoadLatency(duration);
            }
        }

        private static class SyntheticLeaf implements Leaf {
            private final SyntheticFieldLoader loader;
            private final SyntheticFieldLoader.DocValuesLoader docValuesLoader;
            private final Map<String, SyntheticFieldLoader.StoredFieldLoader> storedFieldLoaders;

            private SyntheticLeaf(SyntheticFieldLoader loader, SyntheticFieldLoader.DocValuesLoader docValuesLoader) {
                this.loader = loader;
                this.docValuesLoader = docValuesLoader;
                this.storedFieldLoaders = Map.copyOf(
                    loader.storedFieldLoaders().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
                );
            }

            @Override
            public Source source(LeafStoredFieldLoader storedFieldLoader, int docId) throws IOException {
                try (XContentBuilder b = new XContentBuilder(JsonXContent.jsonXContent, new ByteArrayOutputStream())) {
                    write(storedFieldLoader, docId, b);
                    return Source.fromBytes(BytesReference.bytes(b), b.contentType());
                }
            }

            @Override
            public void write(LeafStoredFieldLoader storedFieldLoader, int docId, XContentBuilder b) throws IOException {
                for (var fieldLevelStoredFieldLoader : storedFieldLoaders.values()) {
                    fieldLevelStoredFieldLoader.advanceToDoc(docId);
                }

                // Maps the names of existing objects to lists of ignored fields they contain.
                Map<String, List<IgnoredSourceFieldMapper.NameValue>> objectsWithIgnoredFields = null;

                for (Map.Entry<String, List<Object>> e : storedFieldLoader.storedFields().entrySet()) {
                    SyntheticFieldLoader.StoredFieldLoader loader = storedFieldLoaders.get(e.getKey());
                    if (loader != null) {
                        loader.load(e.getValue());
                    }
                    if (IgnoredSourceFieldMapper.NAME.equals(e.getKey())) {
                        for (Object value : e.getValue()) {
                            if (objectsWithIgnoredFields == null) {
                                objectsWithIgnoredFields = new HashMap<>();
                            }
                            IgnoredSourceFieldMapper.NameValue nameValue = IgnoredSourceFieldMapper.decode(value);
                            objectsWithIgnoredFields.computeIfAbsent(nameValue.getParentFieldName(), k -> new ArrayList<>()).add(nameValue);
                        }
                    }
                }
                if (objectsWithIgnoredFields != null) {
                    loader.setIgnoredValues(objectsWithIgnoredFields);
                }
                if (docValuesLoader != null) {
                    docValuesLoader.advanceToDoc(docId);
                }
                // TODO accept a requested xcontent type
                if (loader.hasValue()) {
                    loader.write(b);
                } else {
                    b.startObject().endObject();
                }
            }
        }
    }

    /**
     * Load a field for {@link Synthetic}.
     * <p>
     * {@link SyntheticFieldLoader}s load values through objects vended
     * by their {@link #storedFieldLoaders} and {@link #docValuesLoader}
     * methods. Then you call {@link #write} to write the values to an
     * {@link XContentBuilder} which also clears them.
     * <p>
     * This two loaders and one writer setup is specifically designed to
     * efficiently load the {@code _source} of indices that have thousands
     * of fields declared in the mapping but that only have values for
     * dozens of them. It handles this in a few ways:
     * <ul>
     *     <li>{@link #docValuesLoader} must be called once per document
     *         per field to load the doc values, but detects up front if
     *         there are no doc values for that field. It's linear with
     *         the number of fields, whether or not they have values,
     *         but skips entirely missing fields.</li>
     *     <li>{@link #storedFieldLoaders} are only called when the
     *         document contains a stored field with the appropriate name.
     *         So it's fine to have thousands of these declared in the
     *         mapping and you don't really pay much to load them. Just
     *         the cost to build {@link Map} used to address them.</li>
     *     <li>Object fields that don't have any values loaded by either
     *         means bail out of the loading process and don't pass
     *         control down to any of their children. Thus it's fine
     *         to declare huge object structures in the mapping and
     *         you only spend time iterating the ones you need. Or that
     *         have doc values.</li>
     * </ul>
     */
    interface SyntheticFieldLoader {
        /**
         * Load no values.
         */
        SyntheticFieldLoader NOTHING = new SyntheticFieldLoader() {
            @Override
            public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
                return Stream.of();
            }

            @Override
            public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
                return null;
            }

            @Override
            public boolean hasValue() {
                return false;
            }

            @Override
            public void write(XContentBuilder b) {}

            @Override
            public String fieldName() {
                return "";
            }
        };

        /**
         * A {@link Stream} mapping stored field paths to a place to put them
         * so they can be included in the next document.
         */
        Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders();

        /**
         * Build something to load doc values for this field or return
         * {@code null} if there are no doc values for this field to
         * load.
         *
         * @param docIdsInLeaf can be null.
         */
        DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException;

        /**
         * Has this field loaded any values for this document?
         */
        boolean hasValue();

        /**
         * Write values for this document.
         */
        void write(XContentBuilder b) throws IOException;

        /**
         * Allows for identifying and tracking additional field values to include in the field source.
         * @param objectsWithIgnoredFields maps object names to lists of fields they contain with special source handling
         * @return true if any matching fields are identified
         */
        default boolean setIgnoredValues(Map<String, List<IgnoredSourceFieldMapper.NameValue>> objectsWithIgnoredFields) {
            return false;
        }

        /**
         * Returns the canonical field name for this loader.
         */
        String fieldName();

        /**
         * Sync for stored field values.
         */
        interface StoredFieldLoader {
            /**
             * Signals the loader that values for this document will be loaded next.
             * Allows loader to discard cached data for previous document.
             */
            void advanceToDoc(int docId);

            /**
             * Loads values read from a corresponding stored field into this loader.
             */
            void load(List<Object> values);
        }

        /**
         * Loads doc values for a field.
         */
        interface DocValuesLoader {
            /**
             * Load the doc values for this field.
             *
             * @return whether or not there are any values for this field
             */
            boolean advanceToDoc(int docId) throws IOException;
        }
    }

}
