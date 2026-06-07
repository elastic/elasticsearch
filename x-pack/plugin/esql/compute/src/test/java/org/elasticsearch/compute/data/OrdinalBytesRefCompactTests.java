/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasables;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class OrdinalBytesRefCompactTests extends SerializationTestCase {

    public void testCompactOnCleanBlockReturnsSelf() {
        try (OrdinalBytesRefBlock block = buildBlockWithPhantoms(false)) {
            assertThat(block.needsCompaction(), is(false));
            try (OrdinalBytesRefBlock compact = block.compact()) {
                assertThat(compact, sameInstance(block));
                assertThat(compact.needsCompaction(), is(false));
            }
        }
    }

    public void testCompactOnFilteredBlockProducesPhantomFreeDict() {
        // Dictionary: a, phantom-1, b, phantom-2, c. Ords reference 0, 2, 4 only.
        try (
            BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(5);
            IntBlock.Builder ordsBuilder = blockFactory.newIntBlockBuilder(4)
        ) {
            dictBuilder.appendBytesRef(new BytesRef("a"));
            dictBuilder.appendBytesRef(new BytesRef("phantom-1"));
            dictBuilder.appendBytesRef(new BytesRef("b"));
            dictBuilder.appendBytesRef(new BytesRef("phantom-2"));
            dictBuilder.appendBytesRef(new BytesRef("c"));
            ordsBuilder.appendInt(0);
            ordsBuilder.appendNull();
            ordsBuilder.appendInt(2);
            ordsBuilder.appendInt(4);
            try (OrdinalBytesRefBlock block = new OrdinalBytesRefBlock(ordsBuilder.build(), dictBuilder.build(), true)) {
                try (OrdinalBytesRefBlock compact = block.compact()) {
                    assertThat(compact.needsCompaction(), is(false));
                    assertThat(compact.getDictionaryVector().getPositionCount(), equalTo(3));
                    BytesRef scratch = new BytesRef();
                    assertThat(compact.getDictionaryVector().getBytesRef(0, scratch).utf8ToString(), equalTo("a"));
                    assertThat(compact.getDictionaryVector().getBytesRef(1, scratch).utf8ToString(), equalTo("b"));
                    assertThat(compact.getDictionaryVector().getBytesRef(2, scratch).utf8ToString(), equalTo("c"));

                    assertThat(compact.isNull(0), is(false));
                    assertThat(compact.getBytesRef(0, scratch).utf8ToString(), equalTo("a"));
                    assertThat(compact.isNull(1), is(true));
                    assertThat(compact.getBytesRef(2, scratch).utf8ToString(), equalTo("b"));
                    assertThat(compact.getBytesRef(3, scratch).utf8ToString(), equalTo("c"));
                }
            }
        }
    }

    public void testCompactWhenAllReferencedReturnsSameChildren() {
        // Flag is true but ordinals happen to reference every dict entry: short-circuit, no copy.
        try (
            BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(2);
            IntBlock.Builder ordsBuilder = blockFactory.newIntBlockBuilder(3)
        ) {
            dictBuilder.appendBytesRef(new BytesRef("a"));
            dictBuilder.appendBytesRef(new BytesRef("b"));
            ordsBuilder.appendInt(0);
            ordsBuilder.appendInt(1);
            ordsBuilder.appendInt(0);
            IntBlock ords = ordsBuilder.build();
            BytesRefVector dict = dictBuilder.build();
            try (OrdinalBytesRefBlock block = new OrdinalBytesRefBlock(ords, dict, true)) {
                try (OrdinalBytesRefBlock compact = block.compact()) {
                    assertThat(compact, not(sameInstance(block)));
                    assertThat(compact.needsCompaction(), is(false));
                    assertThat(compact.getOrdinalsBlock(), sameInstance(ords));
                    assertThat(compact.getDictionaryVector(), sameInstance(dict));
                }
            }
        }
    }

    public void testFilterThenCompactValuesMatch() {
        try (
            BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(4);
            IntVector.Builder ordsBuilder = blockFactory.newIntVectorBuilder(6)
        ) {
            for (int i = 0; i < 4; i++) {
                dictBuilder.appendBytesRef(new BytesRef("v" + i));
            }
            int[] sourceOrds = new int[] { 0, 3, 1, 2, 0, 3 };
            for (int o : sourceOrds) {
                ordsBuilder.appendInt(o);
            }
            try (OrdinalBytesRefVector source = new OrdinalBytesRefVector(ordsBuilder.build(), dictBuilder.build())) {
                int[] keep = new int[] { 1, 4, 5 }; // values v3, v0, v3
                try (BytesRefVector filtered = source.filter(true, keep)) {
                    OrdinalBytesRefVector ord = (OrdinalBytesRefVector) filtered;
                    assertThat(ord.needsCompaction(), is(true));
                    try (OrdinalBytesRefVector compact = ord.compact()) {
                        assertThat(compact.needsCompaction(), is(false));
                        // Only dictionary entries 0 and 3 are referenced.
                        assertThat(compact.getDictionaryVector().getPositionCount(), equalTo(2));
                        BytesRef scratch = new BytesRef();
                        assertThat(compact.getBytesRef(0, scratch).utf8ToString(), equalTo("v3"));
                        assertThat(compact.getBytesRef(1, scratch).utf8ToString(), equalTo("v0"));
                        assertThat(compact.getBytesRef(2, scratch).utf8ToString(), equalTo("v3"));
                    }
                }
            }
        }
    }

    public void testSerializeRoundTripCompactsBlockDictionary() throws IOException {
        // Dense ordinal block (12 values, 5-entry dict with 2 phantoms) -> the wire path picks
        // the SERIALIZE_BLOCK_ORDINAL branch, which is where we want compaction to happen.
        try (OrdinalBytesRefBlock block = buildDenseBlockWithPhantoms()) {
            assertThat(block.needsCompaction(), is(true));
            assertThat(block.isDense(), is(true));
            assertThat(block.getDictionaryVector().getPositionCount(), equalTo(5));
            try (BytesRefBlock deser = serializeDeserializeBlock((BytesRefBlock) block)) {
                OrdinalBytesRefBlock ord = deser.asOrdinals();
                assertNotNull("expected dictionary-encoded block on the wire", ord);
                assertThat(ord.needsCompaction(), is(false));
                assertThat(ord.getDictionaryVector().getPositionCount(), equalTo(3));
                BytesRef scratch = new BytesRef();
                assertThat(ord.getDictionaryVector().getBytesRef(0, scratch).utf8ToString(), equalTo("a"));
                assertThat(ord.getDictionaryVector().getBytesRef(1, scratch).utf8ToString(), equalTo("b"));
                assertThat(ord.getDictionaryVector().getBytesRef(2, scratch).utf8ToString(), equalTo("c"));
                // Original ords were [0, null, 2, 4, 0, 0, 2, 0, 4, 4, 2, 0]; remapped to
                // [0, null, 1, 2, 0, 0, 1, 0, 2, 2, 1, 0].
                assertThat(ord.isNull(1), is(true));
                assertThat(ord.getBytesRef(0, scratch).utf8ToString(), equalTo("a"));
                assertThat(ord.getBytesRef(2, scratch).utf8ToString(), equalTo("b"));
                assertThat(ord.getBytesRef(3, scratch).utf8ToString(), equalTo("c"));
                assertThat(ord.getBytesRef(8, scratch).utf8ToString(), equalTo("c"));
            }
        }
    }

    public void testSerializeRoundTripCompactsVectorDictionary() throws IOException {
        try (OrdinalBytesRefVector vector = buildDenseVectorWithPhantoms()) {
            assertThat(vector.needsCompaction(), is(true));
            assertThat(vector.isDense(), is(true));
            assertThat(vector.getDictionaryVector().getPositionCount(), equalTo(4));
            try (BytesRefBlock deser = serializeDeserializeBlock((BytesRefBlock) vector.asBlock())) {
                OrdinalBytesRefBlock ord = deser.asOrdinals();
                assertNotNull("expected dictionary-encoded block on the wire", ord);
                assertThat(ord.needsCompaction(), is(false));
                assertThat(ord.getDictionaryVector().getPositionCount(), equalTo(2));
                BytesRef scratch = new BytesRef();
                assertThat(ord.getDictionaryVector().getBytesRef(0, scratch).utf8ToString(), equalTo("a"));
                assertThat(ord.getDictionaryVector().getBytesRef(1, scratch).utf8ToString(), equalTo("c"));
            }
        }
    }

    public void testCompactVectorOnCleanReturnsSelf() {
        try (OrdinalBytesRefVector vector = buildVectorWithPhantoms(false)) {
            assertThat(vector.needsCompaction(), is(false));
            try (OrdinalBytesRefVector compact = vector.compact()) {
                assertThat(compact, sameInstance(vector));
            }
        }
    }

    public void testCompactVectorPhantomFreeDict() {
        try (OrdinalBytesRefVector vector = buildVectorWithPhantoms(true)) {
            assertThat(vector.needsCompaction(), is(true));
            try (OrdinalBytesRefVector compact = vector.compact()) {
                assertThat(compact.needsCompaction(), is(false));
                assertThat(compact.getDictionaryVector().getPositionCount(), equalTo(2));
                BytesRef scratch = new BytesRef();
                assertThat(compact.getBytesRef(0, scratch).utf8ToString(), equalTo("a"));
                assertThat(compact.getBytesRef(1, scratch).utf8ToString(), equalTo("c"));
                assertThat(compact.getBytesRef(2, scratch).utf8ToString(), equalTo("a"));
            }
        }
    }

    public void testCompactVectorAllReferencedReturnsSameChildren() {
        try (
            BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(2);
            IntVector.Builder ordsBuilder = blockFactory.newIntVectorBuilder(3)
        ) {
            dictBuilder.appendBytesRef(new BytesRef("a"));
            dictBuilder.appendBytesRef(new BytesRef("b"));
            ordsBuilder.appendInt(0);
            ordsBuilder.appendInt(1);
            ordsBuilder.appendInt(0);
            IntVector ords = ordsBuilder.build();
            BytesRefVector dict = dictBuilder.build();
            try (OrdinalBytesRefVector vector = new OrdinalBytesRefVector(ords, dict, true)) {
                try (OrdinalBytesRefVector compact = vector.compact()) {
                    assertThat(compact, not(sameInstance(vector)));
                    assertThat(compact.needsCompaction(), is(false));
                    assertThat(compact.getOrdinalsVector(), sameInstance(ords));
                    assertThat(compact.getDictionaryVector(), sameInstance(dict));
                }
            }
        }
    }

    /**
     * Builds a block whose dictionary always has exactly five entries [a, phantom-1, b, phantom-2, c]
     * and whose ordinals reference 0, null, 2, 4. When {@code needsCompaction} is true the result is
     * flagged for compaction; otherwise the (correct but ill-conformed) flag stays false to test the
     * no-op path.
     */
    private OrdinalBytesRefBlock buildBlockWithPhantoms(boolean needsCompaction) {
        BytesRefVector dict = null;
        IntBlock ords = null;
        try (
            BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(5);
            IntBlock.Builder ordsBuilder = blockFactory.newIntBlockBuilder(4)
        ) {
            dictBuilder.appendBytesRef(new BytesRef("a"));
            dictBuilder.appendBytesRef(new BytesRef("phantom-1"));
            dictBuilder.appendBytesRef(new BytesRef("b"));
            dictBuilder.appendBytesRef(new BytesRef("phantom-2"));
            dictBuilder.appendBytesRef(new BytesRef("c"));
            ordsBuilder.appendInt(0);
            ordsBuilder.appendNull();
            ordsBuilder.appendInt(2);
            ordsBuilder.appendInt(4);
            dict = dictBuilder.build();
            ords = ordsBuilder.build();
            OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ords, dict, needsCompaction);
            ords = null;
            dict = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(ords, dict);
        }
    }

    /**
     * Same dictionary as {@link #buildBlockWithPhantoms} but with enough ordinals to satisfy
     * {@link OrdinalBytesRefBlock#isDense()}, so the wire path actually picks the ordinal branch.
     */
    private OrdinalBytesRefBlock buildDenseBlockWithPhantoms() {
        BytesRefVector dict = null;
        IntBlock ords = null;
        try (
            BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(5);
            IntBlock.Builder ordsBuilder = blockFactory.newIntBlockBuilder(12)
        ) {
            dictBuilder.appendBytesRef(new BytesRef("a"));
            dictBuilder.appendBytesRef(new BytesRef("phantom-1"));
            dictBuilder.appendBytesRef(new BytesRef("b"));
            dictBuilder.appendBytesRef(new BytesRef("phantom-2"));
            dictBuilder.appendBytesRef(new BytesRef("c"));
            int[] sequence = new int[] { 0, -1, 2, 4, 0, 0, 2, 0, 4, 4, 2, 0 };
            for (int o : sequence) {
                if (o < 0) {
                    ordsBuilder.appendNull();
                } else {
                    ordsBuilder.appendInt(o);
                }
            }
            dict = dictBuilder.build();
            ords = ordsBuilder.build();
            OrdinalBytesRefBlock result = new OrdinalBytesRefBlock(ords, dict, true);
            ords = null;
            dict = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(ords, dict);
        }
    }

    /**
     * Same dictionary as {@link #buildVectorWithPhantoms} but dense.
     */
    private OrdinalBytesRefVector buildDenseVectorWithPhantoms() {
        BytesRefVector dict = null;
        IntVector ords = null;
        try (
            BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(4);
            IntVector.Builder ordsBuilder = blockFactory.newIntVectorBuilder(10)
        ) {
            dictBuilder.appendBytesRef(new BytesRef("a"));
            dictBuilder.appendBytesRef(new BytesRef("phantom"));
            dictBuilder.appendBytesRef(new BytesRef("b"));
            dictBuilder.appendBytesRef(new BytesRef("c"));
            for (int o : new int[] { 0, 3, 0, 0, 3, 0, 3, 0, 3, 0 }) {
                ordsBuilder.appendInt(o);
            }
            dict = dictBuilder.build();
            ords = ordsBuilder.build();
            OrdinalBytesRefVector result = new OrdinalBytesRefVector(ords, dict, true);
            ords = null;
            dict = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(ords, dict);
        }
    }

    /**
     * Builds a vector with dictionary [a, phantom, b, c] and ordinals 0, 3, 0 (so phantom and "b" are
     * unreferenced).
     */
    private OrdinalBytesRefVector buildVectorWithPhantoms(boolean needsCompaction) {
        BytesRefVector dict = null;
        IntVector ords = null;
        try (
            BytesRefVector.Builder dictBuilder = blockFactory.newBytesRefVectorBuilder(4);
            IntVector.Builder ordsBuilder = blockFactory.newIntVectorBuilder(3)
        ) {
            dictBuilder.appendBytesRef(new BytesRef("a"));
            dictBuilder.appendBytesRef(new BytesRef("phantom"));
            dictBuilder.appendBytesRef(new BytesRef("b"));
            dictBuilder.appendBytesRef(new BytesRef("c"));
            ordsBuilder.appendInt(0);
            ordsBuilder.appendInt(3);
            ordsBuilder.appendInt(0);
            dict = dictBuilder.build();
            ords = ordsBuilder.build();
            OrdinalBytesRefVector result = new OrdinalBytesRefVector(ords, dict, needsCompaction);
            ords = null;
            dict = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(ords, dict);
        }
    }
}
