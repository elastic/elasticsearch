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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterators;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene50.Lucene50PostingsFormat;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BloomFilter;
import org.elasticsearch.index.codec.postingsformat.BloomFilterPostingsFormat.BloomFilteredFieldsConsumer;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

import java.io.IOException;
import java.util.Iterator;

/**
 * This is the old default postings format for Elasticsearch that special cases
 * the <tt>_uid</tt> field to use a bloom filter while all other fields
 * will use a {@link Lucene50PostingsFormat}. This format will reuse the underlying
 * {@link Lucene50PostingsFormat} and its files also for the <tt>_uid</tt> saving up to
 * 5 files per segment in the default case.
 * <p>
 * @deprecated only for reading old segments
 */
@Deprecated
public class Elasticsearch090PostingsFormat extends PostingsFormat {
    protected final BloomFilterPostingsFormat bloomPostings;

    public Elasticsearch090PostingsFormat() {
        super("es090");
        Lucene50PostingsFormat delegate = new Lucene50PostingsFormat();
        assert delegate.getName().equals(Lucene.LATEST_POSTINGS_FORMAT);
        bloomPostings = new BloomFilterPostingsFormat(delegate, BloomFilter.Factory.DEFAULT);
    }

    public PostingsFormat getDefaultWrapped() {
        return bloomPostings.getDelegate();
    }
    protected static final Predicate<String> UID_FIELD_FILTER = new Predicate<String>() {

        @Override
        public boolean apply(String s) {
            return  UidFieldMapper.NAME.equals(s);
        }
    };

    @Override
    public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        throw new UnsupportedOperationException("this codec can only be used for reading");
    }

    @Override
    public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
        // we can just return the delegate here since we didn't record bloom filters for 
        // the other fields. 
        return bloomPostings.fieldsProducer(state);
    }

}
