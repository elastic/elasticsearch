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

package org.elasticsearch.index.shard;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

// TODO: move this to lucene's FilterCodecReader

/** 
 * Base class for filtering DocValuesProducer implementations.
 * <p>
 * NOTE: just like with DocValuesProducer, the default {@link #getMergeInstance()} 
 * is unoptimized. overriding this method when possible can improve performance.
 */
class FilterDocValuesProducer extends DocValuesProducer {
    /** The underlying Producer instance. */
    protected final DocValuesProducer in;
    
    /**
     * Creates a new FilterDocValuesProducer
     * @param in the underlying producer.
     */
    FilterDocValuesProducer(DocValuesProducer in) {
        this.in = in;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public long ramBytesUsed() {
        return in.ramBytesUsed();
    }

    @Override
    public Collection<Accountable> getChildResources() {
        return in.getChildResources();
    }

    @Override
    public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return in.getNumeric(field);
    }

    @Override
    public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return in.getBinary(field);
    }

    @Override
    public SortedDocValues getSorted(FieldInfo field) throws IOException {
        return in.getSorted(field);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        return in.getSortedNumeric(field);
    }

    @Override
    public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        return in.getSortedSet(field);
    }

    @Override
    public Bits getDocsWithField(FieldInfo field) throws IOException {
        return in.getDocsWithField(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
        in.checkIntegrity();
    }
    
    // TODO: move this out somewhere else (and can fix all these null producers in lucene?) 
    // we shouldn't need nullness for any reason.
    
    public static final DocValuesProducer EMPTY = new DocValuesProducer() {

        @Override
        public void close() throws IOException {}

        @Override
        public long ramBytesUsed() {
            return 0;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
        
        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            throw new IllegalStateException(); // we don't have any docvalues
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) throws IOException {
            throw new IllegalStateException(); // we don't have any docvalues
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) throws IOException {
            throw new IllegalStateException(); // we don't have any docvalues
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            throw new IllegalStateException(); // we don't have any docvalues
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
            throw new IllegalStateException(); // we don't have any docvalues
        }

        @Override
        public Bits getDocsWithField(FieldInfo field) throws IOException {
            throw new IllegalStateException(); // we don't have any docvalues
        }

        @Override
        public void checkIntegrity() throws IOException {}
    };
}
