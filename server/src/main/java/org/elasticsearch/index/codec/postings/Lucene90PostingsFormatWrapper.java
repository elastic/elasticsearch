/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.postings;

import org.apache.lucene.backward_codecs.lucene90.Lucene90PostingsFormat;
import org.apache.lucene.backward_codecs.lucene90.Lucene90PostingsWriter;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.PostingsWriterBase;
import org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SuppressForbidden;

import java.io.IOException;

import static org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter.DEFAULT_MAX_BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene90.blocktree.Lucene90BlockTreeTermsWriter.DEFAULT_MIN_BLOCK_SIZE;

/** Wraps Lucene90PostingsFormat in order to enable writing. */
public final class Lucene90PostingsFormatWrapper extends PostingsFormat {

    private final PostingsFormat delegate;

    public static PostingsFormat newInstance() {
        return new Lucene90PostingsFormatWrapper(new Lucene90PostingsFormat());
    }

    private Lucene90PostingsFormatWrapper(PostingsFormat postingsFormat) {
        super(postingsFormat.getName());
        this.delegate = postingsFormat;
    }

    @SuppressForbidden(reason = "consistency with how Lucene cleans up, use IOUtils")
    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        PostingsWriterBase postingsWriter = new Lucene90PostingsWriter(state);
        boolean success = false;
        try {
            FieldsConsumer ret = new Lucene90BlockTreeTermsWriter(state, postingsWriter, DEFAULT_MIN_BLOCK_SIZE, DEFAULT_MAX_BLOCK_SIZE);
            success = true;
            return ret;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(postingsWriter);
            }
        }
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        return delegate.fieldsProducer(state);
    }

    @Override
    public String toString() {
        return "Lucene90PostingsFormatWrapper(delegate=" + delegate + ")";
    }
}
