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

import org.apache.lucene.backward_codecs.lucene50.Lucene50PostingsReader;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsReaderBase;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xpack.lucene.bwc.codecs.lucene40.blocktree.Lucene40BlockTreeTermsReader;

import java.io.IOException;

/**
 * Lucene 5.0 postings format, which encodes postings in packed integer blocks for fast decode.
 *
 *  This is a fork of {@link org.apache.lucene.backward_codecs.lucene50.Lucene50PostingsFormat} that allows to read from indices
 *  written by versions older than Lucene 7.0.
 */
public class BWCLucene50PostingsFormat extends PostingsFormat {

    /**
     * Filename extension for document number, frequencies, and skip data. See chapter: <a
     * href="#Frequencies">Frequencies and Skip Data</a>
     */
    public static final String DOC_EXTENSION = "doc";

    /** Filename extension for positions. See chapter: <a href="#Positions">Positions</a> */
    public static final String POS_EXTENSION = "pos";

    /**
     * Filename extension for payloads and offsets. See chapter: <a href="#Payloads">Payloads and
     * Offsets</a>
     */
    public static final String PAY_EXTENSION = "pay";

    /**
     * Expert: The maximum number of skip levels. Smaller values result in slightly smaller indexes,
     * but slower skipping in big posting lists.
     */
    static final int MAX_SKIP_LEVELS = 10;

    static final String TERMS_CODEC = "Lucene50PostingsWriterTerms";
    static final String DOC_CODEC = "Lucene50PostingsWriterDoc";
    static final String POS_CODEC = "Lucene50PostingsWriterPos";
    static final String PAY_CODEC = "Lucene50PostingsWriterPay";

    // Increment version to change it
    static final int VERSION_START = 0;
    static final int VERSION_IMPACT_SKIP_DATA = 1;
    static final int VERSION_CURRENT = VERSION_IMPACT_SKIP_DATA;

    /** Fixed packed block size, number of integers encoded in a single packed block. */
    // NOTE: must be multiple of 64 because of PackedInts long-aligned encoding/decoding
    public static final int BLOCK_SIZE = 128;

    /** Creates {@code Lucene50PostingsFormat} with default settings. */
    public BWCLucene50PostingsFormat() {
        super("Lucene50");
    }

    public BWCLucene50PostingsFormat(String name) {
        super(name);
    }

    @Override
    public String toString() {
        return getName() + "(blocksize=" + BLOCK_SIZE + ")";
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        throw new UnsupportedOperationException("Old formats can't be used for writing");
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        // The postings reader is the original one from Lucene 5.0
        PostingsReaderBase postingsReader = new Lucene50PostingsReader(state);
        boolean success = false;
        try {
            // The Lucene40BlockTreeTermsReader is a fork
            FieldsProducer ret = new Lucene40BlockTreeTermsReader(postingsReader, state);
            success = true;
            return ret;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(postingsReader);
            }
        }
    }
}
