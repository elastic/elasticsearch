/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.CloseableIterator;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.sameInstance;

public class NoConfigFormatReaderTests extends ESTestCase {

    public void testDefaultReturnsConfiguredEmptyForNullConfig() {
        Stub stub = new Stub();
        Configured<FormatReader> result = stub.withConfigTrackingConsumedKeys(null);
        assertThat(result.value(), sameInstance(stub));
        assertThat(result.consumedKeys(), empty());
    }

    public void testDefaultReturnsConfiguredEmptyForEmptyConfig() {
        Stub stub = new Stub();
        Configured<FormatReader> result = stub.withConfigTrackingConsumedKeys(Map.of());
        assertThat(result.value(), sameInstance(stub));
        assertThat(result.consumedKeys(), empty());
    }

    public void testDefaultReturnsConfiguredEmptyForNonEmptyConfig() {
        // Non-empty config still produces an empty consumed-keys set — the marker contract is
        // "this reader claims no keys"; the coordinator-side validator is what rejects unknowns.
        Stub stub = new Stub();
        Configured<FormatReader> result = stub.withConfigTrackingConsumedKeys(Map.of("foo", "bar", "baz", 42));
        assertThat(result.value(), sameInstance(stub));
        assertThat(result.consumedKeys(), empty());
    }

    public void testDefaultPropagatesThroughSegmentableSubtype() {
        // Diamond-style inheritance: a class implementing both SegmentableFormatReader and
        // NoConfigFormatReader picks up the marker's default rather than the abstract from
        // FormatReader. This pins that contract — important because the public API depends on
        // implementers being able to opt in via a single extra interface declaration.
        SegmentableStub stub = new SegmentableStub();
        Configured<FormatReader> result = stub.withConfigTrackingConsumedKeys(Map.of("a", 1));
        assertThat(result.value(), sameInstance(stub));
        assertThat(result.consumedKeys(), empty());
    }

    private static class Stub implements NoConfigFormatReader {
        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Page next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "noop";
        }

        @Override
        public List<String> fileExtensions() {
            return Collections.emptyList();
        }

        @Override
        public List<Attribute> schema(StorageObject object) throws IOException {
            return Collections.emptyList();
        }

        @Override
        public void close() {}
    }

    private static class SegmentableStub implements SegmentableFormatReader, NoConfigFormatReader {
        @Override
        public long findNextRecordBoundary(InputStream stream) {
            return -1;
        }

        @Override
        public SourceMetadata metadata(StorageObject object) {
            return null;
        }

        @Override
        public CloseableIterator<Page> read(StorageObject object, FormatReadContext context) {
            return new CloseableIterator<>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public Page next() {
                    throw new NoSuchElementException();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public String formatName() {
            return "noop-seg";
        }

        @Override
        public List<String> fileExtensions() {
            return Collections.emptyList();
        }

        @Override
        public List<Attribute> schema(StorageObject object) throws IOException {
            return Collections.emptyList();
        }

        @Override
        public void close() {}
    }
}
