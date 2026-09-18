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
 * Modifications copyright (C) 2026 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.lucene80;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.NormsConsumer;
import org.apache.lucene.codecs.NormsFormat;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;

/**
 * Lucene 8.0 Score normalization format.
 *
 * <p>Encodes normalization values by encoding each value with the minimum number of bytes needed to
 * represent the range (which can be zero).
 *
 * <p>Files:
 *
 * <ol>
 *   <li><code>.nvd</code>: Norms data
 *   <li><code>.nvm</code>: Norms metadata
 * </ol>
 *
 * <ol>
 *   <li><a id="nvm"></a>
 *       <p>The Norms metadata or .nvm file.
 *       <p>For each norms field, this stores metadata, such as the offset into the Norms data
 *       (.nvd)
 *       <p>Norms metadata (.dvm) --&gt; Header,&lt;Entry&gt;<sup>NumFields</sup>,Footer
 *       <ul>
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>Entry --&gt; FieldNumber, DocsWithFieldAddress, DocsWithFieldLength,
 *             NumDocsWithField, BytesPerNorm, NormsAddress
 *         <li>FieldNumber --&gt; {@link DataOutput#writeInt Int32}
 *         <li>DocsWithFieldAddress --&gt; {@link DataOutput#writeLong Int64}
 *         <li>DocsWithFieldLength --&gt; {@link DataOutput#writeLong Int64}
 *         <li>NumDocsWithField --&gt; {@link DataOutput#writeInt Int32}
 *         <li>BytesPerNorm --&gt; {@link DataOutput#writeByte byte}
 *         <li>NormsAddress --&gt; {@link DataOutput#writeLong Int64}
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 *       <p>FieldNumber of -1 indicates the end of metadata.
 *       <p>NormsAddress is the pointer to the start of the data in the norms data (.nvd), or the
 *       singleton value when BytesPerValue = 0. If BytesPerValue is different from 0 then there are
 *       NumDocsWithField values to read at that offset.
 *       <p>DocsWithFieldAddress is the pointer to the start of the bit set containing documents
 *       that have a norm in the norms data (.nvd), or -2 if no documents have a norm value, or -1
 *       if all documents have a norm value.
 *       <p>DocsWithFieldLength is the number of bytes used to encode the set of documents that have
 *       a norm.
 *   <li><a id="nvd"></a>
 *       <p>The Norms data or .nvd file.
 *       <p>For each Norms field, this stores the actual per-document data (the heavy-lifting)
 *       <p>Norms data (.nvd) --&gt; Header,&lt; Data &gt;<sup>NumFields</sup>,Footer
 *       <ul>
 *         <li>Header --&gt; {@link CodecUtil#writeIndexHeader IndexHeader}
 *         <li>DocsWithFieldData --&gt; {@link IndexedDISI#writeBitSet Bit set of MaxDoc bits}
 *         <li>NormsData --&gt; {@link DataOutput#writeByte(byte) byte}<sup>NumDocsWithField *
 *             BytesPerValue</sup>
 *         <li>Footer --&gt; {@link CodecUtil#writeFooter CodecFooter}
 *       </ul>
 * </ol>
 *
 * @lucene.experimental
 */
public class Lucene80NormsFormat extends NormsFormat {

    /** Sole Constructor */
    public Lucene80NormsFormat() {}

    @Override
    public NormsConsumer normsConsumer(SegmentWriteState state) throws IOException {
        throw new UnsupportedOperationException("Old codecs may only be used for reading");
    }

    @Override
    public NormsProducer normsProducer(SegmentReadState state) throws IOException {
        return new Lucene80NormsProducer(state, DATA_CODEC, DATA_EXTENSION, METADATA_CODEC, METADATA_EXTENSION);
    }

    static final String DATA_CODEC = "Lucene80NormsData";
    static final String DATA_EXTENSION = "nvd";
    static final String METADATA_CODEC = "Lucene80NormsMetadata";
    static final String METADATA_EXTENSION = "nvm";
    static final int VERSION_START = 0;
    static final int VERSION_CURRENT = VERSION_START;
}
