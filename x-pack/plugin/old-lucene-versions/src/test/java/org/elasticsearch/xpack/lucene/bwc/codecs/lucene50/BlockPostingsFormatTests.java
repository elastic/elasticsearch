/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2021 Elasticsearch B.V.
 */
package org.elasticsearch.xpack.lucene.bwc.codecs.lucene50;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FreqAndNormBuffer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.BasePostingsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.test.GraalVMThreadsFilter;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene40.blocktree.FieldReader;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene40.blocktree.Stats;

import java.io.IOException;

/** Tests BlockPostingsFormat */
@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class BlockPostingsFormatTests extends BasePostingsFormatTestCase {
    private final Codec codec = TestUtil.alwaysPostingsFormat(new Lucene50RWPostingsFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

    /** Make sure the final sub-block(s) are not skipped. */
    public void testFinalBlock() throws Exception {
        Directory d = newDirectory();
        IndexWriter w = new IndexWriter(d, new IndexWriterConfig(new MockAnalyzer(random())));
        for (int i = 0; i < 25; i++) {
            Document doc = new Document();
            doc.add(newStringField("field", Character.toString((char) (97 + i)), Field.Store.NO));
            doc.add(newStringField("field", "z" + Character.toString((char) (97 + i)), Field.Store.NO));
            w.addDocument(doc);
        }
        w.forceMerge(1);

        DirectoryReader r = DirectoryReader.open(w);
        assertEquals(1, r.leaves().size());
        FieldReader field = (FieldReader) r.leaves().get(0).reader().terms("field");
        // We should see exactly two blocks: one root block (prefix empty string) and one block for z*
        // terms (prefix z):
        Stats stats = field.getStats();
        assertEquals(0, stats.floorBlockCount);
        assertEquals(2, stats.nonFloorBlockCount);
        r.close();
        w.close();
        d.close();
    }

    public void testImpactSerialization() throws IOException {
        // omit norms and omit freqs
        FreqAndNormBuffer freqAndNormBuffer = new FreqAndNormBuffer();
        freqAndNormBuffer.add(1, 1L);
        doTestImpactSerialization(freqAndNormBuffer);

        // omit freqs
        freqAndNormBuffer = new FreqAndNormBuffer();
        freqAndNormBuffer.add(1, 42L);
        doTestImpactSerialization(freqAndNormBuffer);

        // omit freqs with very large norms
        freqAndNormBuffer = new FreqAndNormBuffer();
        freqAndNormBuffer.add(1, -100);
        doTestImpactSerialization(freqAndNormBuffer);

        // omit norms
        freqAndNormBuffer = new FreqAndNormBuffer();
        freqAndNormBuffer.add(30, 1L);
        doTestImpactSerialization(freqAndNormBuffer);

        // omit norms with large freq
        freqAndNormBuffer = new FreqAndNormBuffer();
        freqAndNormBuffer.add(500, 1L);
        doTestImpactSerialization(freqAndNormBuffer);

        // freqs and norms, basic
        freqAndNormBuffer = new FreqAndNormBuffer();
        freqAndNormBuffer.add(1, 7L);
        freqAndNormBuffer.add(3, 9L);
        freqAndNormBuffer.add(7, 10L);
        freqAndNormBuffer.add(15, 11L);
        freqAndNormBuffer.add(20, 13L);
        freqAndNormBuffer.add(28, 14L);
        doTestImpactSerialization(freqAndNormBuffer);

        // freqs and norms, high values
        freqAndNormBuffer = new FreqAndNormBuffer();
        freqAndNormBuffer.add(2, 2L);
        freqAndNormBuffer.add(10, 10L);
        freqAndNormBuffer.add(12, 50L);
        freqAndNormBuffer.add(50, -100L);
        freqAndNormBuffer.add(1000, -80L);
        freqAndNormBuffer.add(1005, -3L);
        doTestImpactSerialization(freqAndNormBuffer);
    }

    private void doTestImpactSerialization(FreqAndNormBuffer impacts) throws IOException {
        CompetitiveImpactAccumulator acc = new CompetitiveImpactAccumulator();
        for (int i = 0; i < impacts.size; i++) {
            acc.add(impacts.freqs[i], impacts.norms[i]);
        }
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = EndiannessReverserUtil.createOutput(dir, "foo", IOContext.DEFAULT)) {
                Lucene50SkipWriter.writeImpacts(acc, out);
            }
            try (IndexInput in = EndiannessReverserUtil.openInput(dir, "foo", IOContext.DEFAULT)) {
                byte[] b = new byte[Math.toIntExact(in.length())];
                in.readBytes(b, 0, b.length);
                FreqAndNormBuffer impacts2 = Lucene50ScoreSkipReader.readImpacts(new ByteArrayDataInput(b), new FreqAndNormBuffer());
                assertEquals(impacts.size, impacts2.size);
                for (int i = 0; i < impacts.size; i++) {
                    assertEquals(impacts.freqs[i], impacts2.freqs[i]);
                    assertEquals(impacts.norms[i], impacts2.norms[i]);
                }
            }
        }
    }
}
