/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class IndexEngineIdLookupPrewarmTests extends ESTestCase {

    /** Index of every leaf (by position in the reader) for which {@code prepareSeekExact} was called, one entry per call. */
    private final List<Integer> preparedLeafOrdinals = new ArrayList<>();

    public void testOnlyLastLeavesArePrewarmed() throws IOException {
        final int segments = IndexEngine.ID_LOOKUP_PREWARM_MAX_LEAVES + randomIntBetween(1, 10);
        final boolean[] hasId = new boolean[segments];
        Arrays.fill(hasId, true);
        prewarm(hasId);

        assertPreparedLeaves(segments - IndexEngine.ID_LOOKUP_PREWARM_MAX_LEAVES, segments);
    }

    public void testAllLeavesPrewarmedWhenNotMoreThanBound() throws IOException {
        final int segments = randomIntBetween(1, IndexEngine.ID_LOOKUP_PREWARM_MAX_LEAVES);
        final boolean[] hasId = new boolean[segments];
        Arrays.fill(hasId, true);
        prewarm(hasId);

        assertPreparedLeaves(0, segments);
    }

    public void testLeavesWithoutIdAreSkippedButCountTowardsTheBound() throws IOException {
        final int segments = IndexEngine.ID_LOOKUP_PREWARM_MAX_LEAVES + 3;
        final boolean[] hasId = new boolean[segments];
        Arrays.fill(hasId, true);
        final int lastWithoutId = segments - 1;
        final int otherWithoutId = segments - 3;
        hasId[lastWithoutId] = false;
        hasId[otherWithoutId] = false;
        prewarm(hasId);

        final var expected = new ArrayList<Integer>();
        for (int i = segments - 1; i >= segments - IndexEngine.ID_LOOKUP_PREWARM_MAX_LEAVES; i--) {
            if (hasId[i]) {
                expected.add(i);
                expected.add(i);
            }
        }
        assertThat(preparedLeafOrdinals, equalTo(expected));
    }

    private void assertPreparedLeaves(int fromInclusive, int toExclusive) {
        final var expected = new ArrayList<Integer>();
        for (int i = toExclusive - 1; i >= fromInclusive; i--) {
            expected.add(i); // min
            expected.add(i); // max
        }
        assertThat(preparedLeafOrdinals, equalTo(expected));
    }

    /** Builds one segment per entry, with or without an {@code _id} field, and prewarms through a reader that records the seeks. */
    private void prewarm(boolean[] segmentHasId) throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                for (int i = 0; i < segmentHasId.length; i++) {
                    for (int d = 0; d < 2; d++) {
                        Document doc = new Document();
                        if (segmentHasId[i]) {
                            doc.add(new StringField(IdFieldMapper.NAME, new BytesRef("id-" + i + "-" + d), Field.Store.NO));
                        } else {
                            doc.add(new StringField("other", "value", Field.Store.NO));
                        }
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }
            try (DirectoryReader reader = new RecordingDirectoryReader(DirectoryReader.open(directory))) {
                assertThat(reader.leaves().size(), equalTo(segmentHasId.length));
                IndexEngine.prewarmIdLookups(reader.leaves());
            }
        }
    }

    /** Wraps every leaf so that {@code prepareSeekExact} calls on the {@code _id} terms are recorded by leaf position. */
    private class RecordingDirectoryReader extends FilterDirectoryReader {
        RecordingDirectoryReader(DirectoryReader in) throws IOException {
            super(in, new SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new RecordingLeafReader(reader);
                }
            });
        }

        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new RecordingDirectoryReader(in);
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private class RecordingLeafReader extends FilterLeafReader {
        RecordingLeafReader(LeafReader in) {
            super(in);
        }

        @Override
        public Terms terms(String field) throws IOException {
            final Terms terms = super.terms(field);
            if (terms == null || IdFieldMapper.NAME.equals(field) == false) {
                return terms;
            }
            return new FilterTerms(terms) {
                @Override
                public TermsEnum iterator() throws IOException {
                    return new FilterTermsEnum(super.iterator()) {
                        @Override
                        public IOBooleanSupplier prepareSeekExact(BytesRef text) throws IOException {
                            preparedLeafOrdinals.add(leafPosition());
                            return super.prepareSeekExact(text);
                        }
                    };
                }
            };
        }

        private int leafPosition() {
            // The leaf's position is encoded in its id values, "id-<segment>-<doc>"
            try {
                final Terms terms = in.terms(IdFieldMapper.NAME);
                final String min = terms.getMin().utf8ToString();
                return Integer.parseInt(min.split("-")[1]);
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }
    }
}
