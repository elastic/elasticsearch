/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

/**
 * The term vectors of a slice index, which hand terms back to the user and read a term's statistics off the dictionary
 * themselves rather than through a field type.
 */
public class SliceTermVectorsTests extends MapperServiceTestCase {

    @Before
    public void requireSliceIndexing() {
        assumeTrue("slice indexing feature flag must be enabled", SliceIndexing.SLICE_FEATURE_FLAG.isEnabled());
    }

    private MapperService sliceMapperService(String termVector) throws IOException {
        final Settings settings = Settings.builder().put(IndexSettings.SLICE_ENABLED.getKey(), true).build();
        return createMapperService(settings, mapping(b -> {
            b.startObject("body").field("type", "text").field("term_vector", termVector).endObject();
            b.startObject("kw").field("type", "keyword").endObject();
        }));
    }

    private static List<String> terms(TermsEnum iterator) throws IOException {
        final List<String> terms = new ArrayList<>();
        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
            terms.add(term.utf8ToString());
        }
        return terms;
    }

    public void testTermVectorsAreHandedBackWithoutTheSlice() throws IOException {
        final MapperService mapperService = sliceMapperService("with_positions_offsets");
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(
                mapperService.documentMapper().parse(source("1", b -> b.field("body", "quick brown fox"), "tenant-a")).rootDoc()
            );
        }, reader -> {
            // What was indexed carries the slice.
            assertThat(
                terms(reader.termVectors().get(0, "body").iterator()),
                contains(
                    SliceIndexing.termPrefix("tenant-a") + "brown",
                    SliceIndexing.termPrefix("tenant-a") + "fox",
                    SliceIndexing.termPrefix("tenant-a") + "quick"
                )
            );
            // What the API hands back does not, and the order survives because a document holds one slice.
            assertThat(
                terms(SliceTermVectors.withoutSlice(reader.termVectors().get(0)).terms("body").iterator()),
                contains("brown", "fox", "quick")
            );
        });
    }

    public void testTermVectorsOfReservedFieldsAreUntouched() throws IOException {
        final MapperService mapperService = sliceMapperService("yes");
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("body", "quick"), "tenant-a")).rootDoc());
        }, reader -> {
            // _id carries no slice, so the stripping view must leave it alone; it has no vector, hence null.
            assertNull(SliceTermVectors.withoutSlice(reader.termVectors().get(0)).terms("_id"));
        });
    }

    /** A plain term says nothing about which slice's copy of it to look up, so a vector is enumerated rather than sought. */
    public void testAVectorCannotBeSoughtByAPlainTerm() throws IOException {
        final MapperService mapperService = sliceMapperService("yes");
        withLuceneIndex(mapperService, writer -> {
            writer.addDocument(mapperService.documentMapper().parse(source("1", b -> b.field("body", "quick"), "tenant-a")).rootDoc());
        }, reader -> {
            final TermsEnum vector = SliceTermVectors.withoutSlice(reader.termVectors().get(0)).terms("body").iterator();
            expectThrows(UnsupportedOperationException.class, () -> vector.seekExact(new BytesRef("quick")));
        });
    }

    /**
     * The statistics reported for a term are the ones of the slice the document is in: the term vectors API looks the
     * plain term it hands back up in the dictionary, which holds a copy of it per slice.
     */
    public void testTheStatisticsOfATermComeFromItsOwnSlice() throws IOException {
        final MapperService mapperService = sliceMapperService("no");
        withLuceneIndex(mapperService, writer -> {
            // "alpha" in one document of tenant-a and two of tenant-b
            for (String id : List.of("a1")) {
                writer.addDocument(mapperService.documentMapper().parse(source(id, b -> b.field("kw", "alpha"), "tenant-a")).rootDoc());
            }
            for (String id : List.of("b1", "b2")) {
                writer.addDocument(mapperService.documentMapper().parse(source(id, b -> b.field("kw", "alpha"), "tenant-b")).rootDoc());
            }
            writer.forceMerge(1);
        }, reader -> {
            final var indexFields = TermVectorsService.fields(reader);
            // The plain term is not in the dictionary, so without the slice there are no statistics to report at all.
            assertFalse(indexFields.terms("kw").iterator().seekExact(new BytesRef("alpha")));

            final TermsEnum inA = SliceTermVectors.inSlice(indexFields, "tenant-a").terms("kw").iterator();
            assertTrue(inA.seekExact(new BytesRef("alpha")));
            assertThat(inA.docFreq(), equalTo(1));
            // and the term it reports is the plain one it was asked for
            assertThat(inA.term().utf8ToString(), equalTo("alpha"));

            final TermsEnum inB = SliceTermVectors.inSlice(indexFields, "tenant-b").terms("kw").iterator();
            assertTrue(inB.seekExact(new BytesRef("alpha")));
            assertThat(inB.docFreq(), equalTo(2));

            // A reserved field is looked up as it stands.
            assertNotNull(SliceTermVectors.inSlice(indexFields, "tenant-a").terms("_id"));
        });
    }
}
