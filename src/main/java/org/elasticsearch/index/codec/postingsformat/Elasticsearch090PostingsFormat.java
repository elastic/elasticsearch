/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.codec.postingsformat;

import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.TermsConsumer;
import org.apache.lucene.codecs.lucene41.Lucene41PostingsFormat;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.index.codec.postingsformat.BloomFilterPostingsFormat.BloomFilteredFieldsConsumer;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;

/**
 * This is the default postings format for Elasticsearch that special cases
 * the <tt>_uid</tt> field to use a bloom filter while all other fields
 * will use a {@link Lucene41PostingsFormat}. This format will reuse the underlying
 * {@link Lucene41PostingsFormat} and its files also for the <tt>_uid</tt> saving up to
 * 5 files per segment in the default case.
 */
public final class Elasticsearch090PostingsFormat extends PostingsFormat {
    private final BloomFilterPostingsFormat bloomPostings;

    public Elasticsearch090PostingsFormat() {
        super("es090");
        bloomPostings = new BloomFilterPostingsFormat(new Lucene41PostingsFormat(), BloomFilter.Factory.DEFAULT);
    }

    public PostingsFormat getDefaultWrapped() {
        return bloomPostings.getDelegate();
    }

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        final BloomFilteredFieldsConsumer fieldsConsumer = bloomPostings.fieldsConsumer(state);
        return new FieldsConsumer() {

            @Override
            public void close() throws IOException {
                fieldsConsumer.close();
            }

            @Override
            public TermsConsumer addField(FieldInfo field) throws IOException {
                if (UidFieldMapper.NAME.equals(field.name)) {
                    // only go through bloom for the UID field
                    return fieldsConsumer.addField(field);
                }
                return fieldsConsumer.getDelegate().addField(field);
            }
        };
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        // we can just return the delegate here since we didn't record bloom filters for 
        // the other fields. 
        return bloomPostings.fieldsProducer(state);
    }

}
