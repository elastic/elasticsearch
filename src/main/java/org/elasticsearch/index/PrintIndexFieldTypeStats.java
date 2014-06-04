package org.elasticsearch.index;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;

// TODO
//   - work on non-ES indices: if no uid field, just use
//     "all" or something and remove from prints
//   - sampling

// From ES dir: java -cp lib/lucene-suggest-4.8.1.jar:lib/lucene-core-4.8.1.jar:lib/elasticsearch-2.0.0-SNAPSHOT.jar org.elasticsearch.index.PrintIndexFieldTypeStats data/elasticsearch/nodes/0/indices/logs/0/index

/** Produces summary stored fields and postings statistics of all type X fields in the index. */
public class PrintIndexFieldTypeStats {

    public static class FieldStats {
        IndexOptions options;
        DocValuesType dvType;
    }

    public static void main(String[] args) throws IOException {
        printFieldStats(new File(args[0]));
    }

    private static class DocToType {
        /** Maps ordinal to _type */
        public final Map<Integer,String> ordToType;

        /** Maps _type to ordinal */
        public final Map<String,Integer> typeToOrd;

        /** Maps doc to _type's ordinal */
        public final PackedInts.Reader docToOrd;

        public DocToType(Map<Integer,String> ordToType, Map<String,Integer> typeToOrd, PackedInts.Reader docToOrd) {
            this.ordToType = ordToType;
            this.typeToOrd = typeToOrd;
            this.docToOrd = docToOrd;
        }
    }

    private static DocToType getDocToType(AtomicReader ar, DocToType lastDocToType) throws IOException {
        Fields fields = ar.fields();
    
        Map<Integer,String> ordToType;
        Map<String,Integer> typeToOrd;
        if (lastDocToType == null) {
            ordToType = new HashMap<>();
            typeToOrd = new HashMap<>();
        } else {
            ordToType = lastDocToType.ordToType;
            typeToOrd = lastDocToType.typeToOrd;
        }

        int maxDoc = ar.maxDoc();
        GrowableWriter docToOrd = new GrowableWriter(1, maxDoc, PackedInts.DEFAULT);
        int curOrd = -1;
        if (fields != null) {
            Map<String,Bits> byType = new HashMap<>();
            Terms terms = fields.terms(UidFieldMapper.NAME);
            TermsEnum termsEnum = terms.iterator(null);
            BytesRef term;
            String lastType = null;
            FixedBitSet currentType = null;
            DocsEnum docsEnum = null;
            while ((term = termsEnum.next()) != null) {
                String termString = term.utf8ToString();
                //System.out.println("  term: " + termString);
                int i = termString.indexOf('#');
                if (i != -1) {
                    String type = termString.substring(0, i);
                    if (lastType == null || type.equals(lastType) == false) {
                        if (typeToOrd.containsKey(type)) {
                            curOrd = typeToOrd.get(type);
                        } else {
                            curOrd = typeToOrd.size();
                            ordToType.put(curOrd, type);
                            typeToOrd.put(type, curOrd);
                        }
                        lastType = type;
                    }
                    docsEnum = termsEnum.docs(null, docsEnum, 0);
                    int docID = docsEnum.nextDoc();
                    assert docID != DocsEnum.NO_MORE_DOCS;
                    assert curOrd != -1;
                    docToOrd.set(docID, curOrd);
                    //System.out.println("  doc " + docID + " -> " + curOrd);
                } else {
                    System.out.println("WARNING: mal-formed uid " + termString);
                }
            }
            return new DocToType(ordToType, typeToOrd, docToOrd);
        } else {
            // shouldn't happen?
            return null;
        }
    }

    private static class FieldPostingsStats {
        public long totPostingsInts;
        public double totTermBytes;
    }

    public static void printFieldStats(File path) throws IOException {
        Directory dir = FSDirectory.open(path);
        IndexReader r = DirectoryReader.open(dir);
        List<FieldInfos> allFieldInfos = new ArrayList<>();
        if (r.maxDoc() == 0) {
            System.out.println("\nIndex is empty");
            r.close();
            return;
        }
        System.out.println("\nReader numSegs=" + r.leaves().size() + " maxDoc=" + r.maxDoc() + " delDocs=" + r.numDeletedDocs() + " (" + (100*r.numDeletedDocs()/r.maxDoc()) + "%) " + r);
        Map<Integer,Map<String,Double>> typeOrdToSourceBytes = new HashMap<>();

        Map<String,FieldPostingsStats[]> fieldToPostingsStats = new HashMap<>();

        DocToType docToType = null;
        for (AtomicReaderContext ctx : r.leaves()) {
            AtomicReader ar = ctx.reader();
            System.out.println("  process seg=" + ar);
            FieldInfos fieldInfos = ar.getFieldInfos();
            allFieldInfos.add(fieldInfos);
            int maxDoc = ar.maxDoc();
            docToType = getDocToType(ar, docToType);

            // Postings
            Fields fields = ar.fields();
            for (String field : fields) {
                Terms terms = fields.terms(field);
                int numTypes = docToType.ordToType.size();
                FieldPostingsStats[] fieldPostingsStats = fieldToPostingsStats.get(field);
                if (fieldPostingsStats == null) {
                    fieldPostingsStats = new FieldPostingsStats[numTypes];
                    fieldToPostingsStats.put(field, fieldPostingsStats);
                } else if (fieldPostingsStats.length < numTypes) {
                    FieldPostingsStats[] newArray = new FieldPostingsStats[ArrayUtil.oversize(docToType.ordToType.size(), RamUsageEstimator.NUM_BYTES_OBJECT_REF)];
                    System.arraycopy(fieldPostingsStats, 0, newArray, 0, fieldPostingsStats.length);
                    fieldPostingsStats = newArray;
                    fieldToPostingsStats.put(field, fieldPostingsStats);
                }

                TermsEnum termsEnum = terms.iterator(null);
                BytesRef term = null;
                //DocsAndPositionsEnum posEnum = null;
                DocsEnum docsEnum = null;

                IndexOptions indexOptions = fieldInfos.fieldInfo(field).getIndexOptions();
                boolean hasPositions = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
                boolean hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
                boolean hasFreqs = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;

                while ((term = termsEnum.next()) != null) {
                    //posEnum = termsEnum.docsAndPositions(null, posEnum, 
                    docsEnum = termsEnum.docs(null, docsEnum, DocsEnum.FLAG_FREQS);

                    int termBytes = term.length;

                    // To properly account for
                    // massive-raw-term-has-only-1-doc we assign
                    // termBytes divided by all docs in the postings:
                    double termBytesPerDoc = termBytes / termsEnum.docFreq();

                    int docID = 0;
                    while ((docID = docsEnum.nextDoc()) != DocsEnum.NO_MORE_DOCS) {
                        int typeOrd = (int) docToType.docToOrd.get(docID);            
                        FieldPostingsStats stats = fieldPostingsStats[typeOrd];
                        if (stats == null) {
                            stats = new FieldPostingsStats();
                            fieldPostingsStats[typeOrd] = stats;
                        }
                        stats.totTermBytes += termBytesPerDoc;

                        int postingsInts;

                        if (hasOffsets) {
                            // docID + freq + positions
                            postingsInts = 2+3*docsEnum.freq();
                        } else if (hasPositions) {
                            // docID + freq + positions
                            postingsInts = 2+docsEnum.freq();
                        } else if (hasFreqs) {
                            // docID + freq
                            postingsInts = 2;
                        } else {
                            // just docID
                            postingsInts = 1;
                        }
            
                        stats.totPostingsInts += postingsInts;
                    }
                }
            }

            // Stored fields:
            for(int docID=0;docID < maxDoc;docID++) {
                Document doc = ar.document(docID);
                int typeOrd = (int) docToType.docToOrd.get(docID);
                Map<String,Double> typeSizes = typeOrdToSourceBytes.get(typeOrd);
                if (typeSizes == null) {
                    typeSizes = new HashMap<>();
                    typeOrdToSourceBytes.put(typeOrd, typeSizes);
                }
                for(IndexableField field : doc.getFields()) {
                    double cur;
                    if (typeSizes.containsKey(field.name()) == false) {
                        cur = 0;
                    } else {
                        cur = typeSizes.get(field.name());
                    }
                    // nocommit this math is wrong
                    long size = field.toString().length()*2;
                    typeSizes.put(field.name(), cur+size);
                }
            }

            System.out.println("\nstored fields bytes by _type & field:");
            printDoubleMap(docToType, typeOrdToSourceBytes, null, true);

            printPostingsStats(docToType, fieldToPostingsStats, allFieldInfos);
        }

        System.out.println("\nstored fields bytes by _type & field:");
        printDoubleMap(docToType, typeOrdToSourceBytes, null, true);

        printPostingsStats(docToType, fieldToPostingsStats, allFieldInfos);

        r.close();
        dir.close();
    }

    private static void printDoubleMap(DocToType docToType, Map<Integer,Map<String,Double>> typeOrdToFieldBytes,
                                       List<FieldInfos> allFieldInfos, boolean isBytes) {
        final List<Map.Entry<Integer,Map<String,Double>>> sizes = new ArrayList<>(typeOrdToFieldBytes.entrySet());
        new InPlaceMergeSorter() {

            @Override
            protected int compare(int i, int j) {
                return Double.compare(sumSizes(sizes.get(j).getValue()), sumSizes(sizes.get(i).getValue()));
            }

            @Override
            protected void swap(int i, int j) {
                Map.Entry<Integer,Map<String,Double>> tmp = sizes.get(i);
                sizes.set(i, sizes.get(j));
                sizes.set(j, tmp);
            }
        }.sort(0, sizes.size());

        double totSize = 0;
        for(Map.Entry<Integer,Map<String,Double>> ent : sizes) {
            totSize += sumSizes(ent.getValue());
        }

        for(Map.Entry<Integer,Map<String,Double>> ent : sizes) {
            final List<Map.Entry<String,Double>> fieldSizes = new ArrayList<>(ent.getValue().entrySet());

            double sumSize = sumSizes(ent.getValue());
            if (isBytes) {
                System.out.println(String.format(Locale.ROOT, "  %s: %.3f%%, %.3f MB (%d fields)",
                                                 docToType.ordToType.get(ent.getKey()),
                                                 100*((double) sumSize)/totSize,
                                                 sumSize/1024/1024.,
                                                 fieldSizes.size()));
            } else {
                System.out.println(String.format(Locale.ROOT, "  %s: %.3f%%, %.3f M (%d fields)",
                                                 docToType.ordToType.get(ent.getKey()),
                                                 100*((double) sumSize)/totSize,
                                                 sumSize/1000000.,
                                                 fieldSizes.size()));
            }

            new InPlaceMergeSorter() {

                @Override
                protected int compare(int i, int j) {
                    return Double.compare(fieldSizes.get(j).getValue(), fieldSizes.get(i).getValue());
                }

                @Override
                protected void swap(int i, int j) {
                    Map.Entry<String,Double> tmp = fieldSizes.get(i);
                    fieldSizes.set(i, fieldSizes.get(j));
                    fieldSizes.set(j, tmp);
                }
            }.sort(0, fieldSizes.size());

            double totFieldSize = sumSizes(ent.getValue());

            for(Map.Entry<String,Double> ent2 : fieldSizes) {
                String fieldName = ent2.getKey();
                StringBuilder b = new StringBuilder();
                if (allFieldInfos != null) {
                    boolean sawNorms = false;
                    boolean sawOmitNorms = false;
                    boolean storeTVs = false;
                    boolean hasPayloads = false;
                    Set<IndexOptions> indexOptions = new HashSet<IndexOptions>();
                    DocValuesType dvType = null;
                    for(FieldInfos fieldInfos : allFieldInfos) {
                        FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldName);
                        if (fieldInfo != null) {
                            dvType = fieldInfo.getDocValuesType();
                            storeTVs |= fieldInfo.hasVectors();
                            hasPayloads |= fieldInfo.hasPayloads();
                            if (fieldInfo.omitsNorms()) {
                                if (sawOmitNorms == false) {
                                    sawOmitNorms = true;
                                    if (b.length() != 0) {
                                        b.append(", ");
                                    }
                                    b.append("no-norms");
                                }
                            } else {
                                if (sawNorms == false) {
                                    sawNorms = true;
                                    if (b.length() != 0) {
                                        b.append(", ");
                                    }
                                    b.append("norms");
                                }
                            }
                            indexOptions.add(fieldInfo.getIndexOptions());
                        }
                    }
                    for(IndexOptions option : indexOptions) {
                        b.append(",");
                        b.append(option);
                    }
                    if (dvType != null) {
                        b.append(",");
                        b.append(dvType);
                    }
                    if (storeTVs) {
                        b.append(",");
                        b.append("term-vectors");
                    }
                    if (hasPayloads) {
                        b.append(",");
                        b.append("payloads");
                    }
                }

                if (isBytes) {
                    System.out.println(String.format(Locale.ROOT, "    %s\n      %.3f%%, %.3f MB",
                                                     fieldName,
                                                     100*((double) ent2.getValue())/totFieldSize,
                                                     ent2.getValue()/1024/1024.));
                } else {
                    System.out.println(String.format(Locale.ROOT, "    %s\n      %s\n      %.3f%%, %.3f M",
                                                     fieldName,
                                                     b.toString(),
                                                     100*((double) ent2.getValue())/totFieldSize,
                                                     ent2.getValue()/1000000.));
                }
            }
        }
    }

    private static void printPostingsStats(DocToType docToType, Map<String,FieldPostingsStats[]> fieldToPostingsStats,
                                           List<FieldInfos> allFieldInfos) {
        // we have map field -> type -> stats, we need to invert
        // it so we can sort by type's overall usage
        Map<Integer,Map<String,Double>> fieldTermBytes = new HashMap<>();
        Map<Integer,Map<String,Double>> fieldPostingsCounts = new HashMap<>();
        for(Map.Entry<String,FieldPostingsStats[]> ent : fieldToPostingsStats.entrySet()) {
            String fieldName = ent.getKey();
            FieldPostingsStats[] stats = ent.getValue();
            for(Map.Entry<Integer,String> ent2 : docToType.ordToType.entrySet()) {
                int typeOrd = ent2.getKey();
                if (typeOrd >= stats.length) {
                    continue;
                }
                FieldPostingsStats typeStats = stats[typeOrd];
                if (typeStats == null) {
                    // expected: this type never saw this field
                    continue;
                }
                Map<String,Double> byField = fieldTermBytes.get(typeOrd);
                if (byField == null) {
                    byField = new HashMap<>();
                    fieldTermBytes.put(typeOrd, byField);
                }
                double curBytes;
                if (byField.containsKey(fieldName)) {
                    curBytes = byField.get(fieldName);
                } else {
                    curBytes = 0;
                }
                curBytes += typeStats.totTermBytes;
                byField.put(fieldName, curBytes);

                byField = fieldPostingsCounts.get(typeOrd);
                if (byField == null) {
                    byField = new HashMap<>();
                    fieldPostingsCounts.put(typeOrd, byField);
                }
                double curPostingsCount;
                if (byField.containsKey(fieldName)) {
                    curPostingsCount = byField.get(fieldName);
                } else {
                    curPostingsCount = 0;
                }
                curPostingsCount += typeStats.totPostingsInts;
                byField.put(fieldName, curPostingsCount);
            }
        }

        System.out.println("\npostings terms bytes:");
        printDoubleMap(docToType, fieldTermBytes, allFieldInfos, true);

        System.out.println("\npostings total ints:");
        printDoubleMap(docToType, fieldPostingsCounts, allFieldInfos, false);
    }

    static double sumSizes(Map<String,Double> map) {
        double sum = 0;
        for(double size : map.values()) {
            sum += size;
        }
        return sum;
    }
}

