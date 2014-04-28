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

package org.elasticsearch.benchmark.fielddata;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.Mapper.BuilderContext;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.indices.fielddata.breaker.DummyCircuitBreakerService;

import java.util.Random;

public class LongFieldDataBenchmark {

    private static final Random RANDOM = new Random();
    private static final int SECONDS_PER_YEAR = 60 * 60 * 24 * 365;

    public static enum Data {
        SINGLE_VALUES_DENSE_ENUM {
            public int numValues() {
                return 1;
            }

            @Override
            public long nextValue() {
                return RANDOM.nextInt(16);
            }
        },
        SINGLE_VALUED_DENSE_DATE {
            public int numValues() {
                return 1;
            }

            @Override
            public long nextValue() {
                // somewhere in-between 2010 and 2012
                return 1000L * (40L * SECONDS_PER_YEAR + RANDOM.nextInt(2 * SECONDS_PER_YEAR));
            }
        },
        MULTI_VALUED_DATE {
            public int numValues() {
                return RANDOM.nextInt(3);
            }

            @Override
            public long nextValue() {
                // somewhere in-between 2010 and 2012
                return 1000L * (40L * SECONDS_PER_YEAR + RANDOM.nextInt(2 * SECONDS_PER_YEAR));
            }
        },
        MULTI_VALUED_ENUM {
            public int numValues() {
                return RANDOM.nextInt(3);
            }

            @Override
            public long nextValue() {
                return 3 + RANDOM.nextInt(8);
            }
        },
        SINGLE_VALUED_SPARSE_RANDOM {
            public int numValues() {
                return RANDOM.nextFloat() < 0.1f ? 1 : 0;
            }

            @Override
            public long nextValue() {
                return RANDOM.nextLong();
            }
        },
        MULTI_VALUED_SPARSE_RANDOM {
            public int numValues() {
                return RANDOM.nextFloat() < 0.1f ? 1 + RANDOM.nextInt(5) : 0;
            }

            @Override
            public long nextValue() {
                return RANDOM.nextLong();
            }
        },
        MULTI_VALUED_DENSE_RANDOM {
            public int numValues() {
                return 1 + RANDOM.nextInt(3);
            }

            @Override
            public long nextValue() {
                return RANDOM.nextLong();
            }
        };

        public abstract int numValues();

        public abstract long nextValue();
    }

    public static void main(String[] args) throws Exception {
        final IndexWriterConfig iwc = new IndexWriterConfig(Lucene.VERSION, new KeywordAnalyzer());
        final String fieldName = "f";
        final int numDocs = 1000000;
        System.out.println("Data\tLoading time\tImplementation\tActual size\tExpected size");
        for (Data data : Data.values()) {
            final RAMDirectory dir = new RAMDirectory();
            final IndexWriter indexWriter = new IndexWriter(dir, iwc);
            for (int i = 0; i < numDocs; ++i) {
                final Document doc = new Document();
                final int numFields = data.numValues();
                for (int j = 0; j < numFields; ++j) {
                    doc.add(new LongField(fieldName, data.nextValue(), Store.NO));
                }
                indexWriter.addDocument(doc);
            }
            indexWriter.forceMerge(1, true);
            indexWriter.close();

            final DirectoryReader dr = DirectoryReader.open(dir);
            final IndexFieldDataService fds = new IndexFieldDataService(new Index("dummy"), new DummyCircuitBreakerService());
            final LongFieldMapper mapper = new LongFieldMapper.Builder(fieldName).build(new BuilderContext(null, new ContentPath(1)));
            final IndexNumericFieldData<AtomicNumericFieldData> fd = fds.getForField(mapper);
            final long start = System.nanoTime();
            final AtomicNumericFieldData afd = fd.loadDirect(SlowCompositeReaderWrapper.wrap(dr).getContext());
            final long loadingTimeMs = (System.nanoTime() - start) / 1000 / 1000;
            System.out.println(data + "\t" + loadingTimeMs + "\t" + afd.getClass().getSimpleName() + "\t" + RamUsageEstimator.humanSizeOf(afd.getLongValues()) + "\t" + RamUsageEstimator.humanReadableUnits(afd.getMemorySizeInBytes()));
            dr.close();
        }
    }

}
