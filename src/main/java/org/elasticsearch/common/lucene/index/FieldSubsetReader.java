package org.elasticsearch.common.lucene.index;

/*
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
 */

import org.apache.lucene.index.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FilterIterator;
import org.elasticsearch.index.mapper.internal.FieldNamesFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

/**
 * A {@link FilterLeafReader} that exposes only a subset
 * of fields from the underlying wrapped reader.
 */
// based on lucene/test-framework's FieldFilterLeafReader.
public final class FieldSubsetReader extends FilterLeafReader {
    
    /**
     * Wraps a provided DirectoryReader. Note that for convenience, the returned reader
     * can be used normally (e.g. passed to {@link DirectoryReader#openIfChanged(DirectoryReader)})
     * and so on. 
     */
    public static DirectoryReader wrap(DirectoryReader in, Set<String> fields) throws IOException {
        return new FieldSubsetDirectoryReader(in, fields);
    }
    
    // wraps subreaders with fieldsubsetreaders.
    static class FieldSubsetDirectoryReader extends FilterDirectoryReader {
        final Set<String> fields;
        
        public FieldSubsetDirectoryReader(DirectoryReader in, final Set<String> fields) throws IOException  {
            super(in, new FilterDirectoryReader.SubReaderWrapper() {
                @Override
                public LeafReader wrap(LeafReader reader) {
                    return new FieldSubsetReader(reader, fields);
                }
            });
            this.fields = fields;
        }
        
        @Override
        protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
            return new FieldSubsetDirectoryReader(in, fields);
        }
    }
    
    // nocommit: special handling for _source and _field_names.
    private final FieldInfos fieldInfos;
    
    public FieldSubsetReader(LeafReader in, Set<String> fields) {
        super(in);
        ArrayList<FieldInfo> filteredInfos = new ArrayList<>();
        for (FieldInfo fi : in.getFieldInfos()) {
            if (fields.contains(fi.name)) {
                filteredInfos.add(fi);
            }
        }
        fieldInfos = new FieldInfos(filteredInfos.toArray(new FieldInfo[filteredInfos.size()]));
    }
    
    boolean hasField(String field) {
        return fieldInfos.fieldInfo(field) != null;
    }
    
    @Override
    public FieldInfos getFieldInfos() {
        return fieldInfos;
    }
    
    @Override
    public Fields getTermVectors(int docID) throws IOException {
        Fields f = super.getTermVectors(docID);
        if (f == null) {
            return null;
        }
        f = new FieldFilterFields(f);
        // we need to check for emptyness, so we can return null:
        return f.iterator().hasNext() ? f : null;
    }
    
    @Override
    public void document(final int docID, final StoredFieldVisitor visitor) throws IOException {
        super.document(docID, new StoredFieldVisitor() {
            @Override
            public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
                visitor.binaryField(fieldInfo, value);
            }
            
            @Override
            public void stringField(FieldInfo fieldInfo, String value) throws IOException {
                visitor.stringField(fieldInfo, value);
            }
            
            @Override
            public void intField(FieldInfo fieldInfo, int value) throws IOException {
                visitor.intField(fieldInfo, value);
            }
            
            @Override
            public void longField(FieldInfo fieldInfo, long value) throws IOException {
                visitor.longField(fieldInfo, value);
            }
            
            @Override
            public void floatField(FieldInfo fieldInfo, float value) throws IOException {
                visitor.floatField(fieldInfo, value);
            }
            
            @Override
            public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
                visitor.doubleField(fieldInfo, value);
            }
            
            @Override
            public Status needsField(FieldInfo fieldInfo) throws IOException {
                return hasField(fieldInfo.name) ? visitor.needsField(fieldInfo) : Status.NO;
            }
        });
    }
    
    @Override
    public Fields fields() throws IOException {
        final Fields f = super.fields();
        return (f == null) ? null : new FieldFilterFields(f);
    }    
    
    @Override
    public NumericDocValues getNumericDocValues(String field) throws IOException {
        return hasField(field) ? super.getNumericDocValues(field) : null;
    }
    
    @Override
    public BinaryDocValues getBinaryDocValues(String field) throws IOException {
        return hasField(field) ? super.getBinaryDocValues(field) : null;
    }
    
    @Override
    public SortedDocValues getSortedDocValues(String field) throws IOException {
        return hasField(field) ? super.getSortedDocValues(field) : null;
    }
    
    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) throws IOException {
        return hasField(field) ? super.getSortedNumericDocValues(field) : null;
    }
    
    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) throws IOException {
        return hasField(field) ? super.getSortedSetDocValues(field) : null;
    }
    
    @Override
    public NumericDocValues getNormValues(String field) throws IOException {
        return hasField(field) ? super.getNormValues(field) : null;
    }
    
    @Override
    public Bits getDocsWithField(String field) throws IOException {
        return hasField(field) ? super.getDocsWithField(field) : null;
    }
    
    private class FieldFilterFields extends FilterFields {
        
        public FieldFilterFields(Fields in) {
            super(in);
        }
        
        @Override
        public int size() {
            // this information is not cheap, return -1 like MultiFields does:
            return -1;
        }
        
        @Override
        public Iterator<String> iterator() {
            return new FilterIterator<String, String>(super.iterator()) {
                @Override
                protected boolean predicateFunction(String field) {
                    return hasField(field);
                }
            };
        }
        
        @Override
        public Terms terms(String field) throws IOException {
            if (FieldNamesFieldMapper.NAME.equals(field)) {
                Terms terms = super.terms(field);
                if (terms != null) {
                    terms = new FieldSubsetTerms(terms, fieldInfos);
                }
                return terms;
            } else if (hasField(field)) {
                return super.terms(field);
            } else {
                return null;
            }
        }
    }
    
    static class FieldSubsetTerms extends FilterTerms {
        final FieldInfos infos;
        
        FieldSubsetTerms(Terms in, FieldInfos infos) {
            super(in);
            this.infos = infos;
        }

        @Override
        public int getDocCount() throws IOException {
            return -1;
        }

        @Override
        public long getSumDocFreq() throws IOException {
            return -1;
        }

        @Override
        public long getSumTotalTermFreq() throws IOException {
            return -1;
        }

        @Override
        public TermsEnum iterator(TermsEnum reuse) throws IOException {
            return new FieldSubsetTermsEnum(in.iterator(null), infos);
        }

        @Override
        public long size() throws IOException {
            return -1;
        }
    }
    
    static class FieldSubsetTermsEnum extends FilterTermsEnum {
        final FieldInfos infos;
        
        FieldSubsetTermsEnum(TermsEnum in, FieldInfos infos) {
            super(in);
            this.infos = infos;
        }
        
        /** Return true if term is accepted.
         */
        protected boolean accept(BytesRef term) throws IOException {
            return infos.fieldInfo(term.utf8ToString()) != null;
        }

        @Override
        public boolean seekExact(BytesRef term) throws IOException {
            return accept(term) && in.seekExact(term);
        }

        @Override
        public SeekStatus seekCeil(BytesRef term) throws IOException {
            SeekStatus status = in.seekCeil(term);
            if (status == SeekStatus.END || accept(term())) {
                return status;
            }
            return next() == null ? SeekStatus.END : SeekStatus.NOT_FOUND;
        }

        @Override
        public void seekExact(long ord) throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public long ord() throws IOException {
          throw new UnsupportedOperationException();
        }

        @Override
        public BytesRef next() throws IOException {
            BytesRef next;
            while ((next = in.next()) != null) {
                if (accept(next)) {
                    break;
                }
            }
            return next;
        }
    }
    
}
