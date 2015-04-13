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

package org.elasticsearch.search.suggest.completion;

import com.google.common.collect.Lists;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.suggest.InputIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.Elasticsearch090PostingsFormat;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.SuggestUtils;
import org.elasticsearch.search.suggest.completion.Completion090PostingsFormat.LookupFactory;
import org.elasticsearch.search.suggest.context.ContextMapping;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CompletionPostingsFormatTest extends ElasticsearchTestCase {
    
    Settings indexSettings = ImmutableSettings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT.id).build();

    @Test
    public void testCompletionPostingsFormat() throws IOException {
        AnalyzingCompletionLookupProviderV1 providerV1 = new AnalyzingCompletionLookupProviderV1(true, false, true, true);
        AnalyzingCompletionLookupProvider currentProvider = new AnalyzingCompletionLookupProvider(true, false, true, true);
        List<Completion090PostingsFormat.CompletionLookupProvider> providers = Lists.newArrayList(providerV1, currentProvider);

        Completion090PostingsFormat.CompletionLookupProvider randomProvider = providers.get(getRandom().nextInt(providers.size()));
        RAMDirectory dir = new RAMDirectory();
        writeData(dir, randomProvider);

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = currentProvider.load(input);
        PostingsFormat format = PostingsFormat.forName(Lucene.LATEST_POSTINGS_FORMAT);
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer());
        Lookup lookup = load.getLookup(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE, indexSettings, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING), new CompletionSuggestionContext(null));
        List<LookupResult> result = lookup.lookup("ge", false, 10);
        assertThat(result.get(0).key.toString(), equalTo("Generator - Foo Fighters"));
        assertThat(result.get(0).payload.utf8ToString(), equalTo("id:10"));
        dir.close();
    }

    @Test
    public void testProviderBackwardCompatibilityForVersion1() throws IOException {
        AnalyzingCompletionLookupProviderV1 providerV1 = new AnalyzingCompletionLookupProviderV1(true, false, true, true);
        AnalyzingCompletionLookupProvider currentProvider = new AnalyzingCompletionLookupProvider(true, false, true, true);

        RAMDirectory dir = new RAMDirectory();
        writeData(dir, providerV1);

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = currentProvider.load(input);
        PostingsFormat format = new Elasticsearch090PostingsFormat();
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer());
        AnalyzingCompletionLookupProvider.AnalyzingSuggestHolder analyzingSuggestHolder = load.getAnalyzingSuggestHolder(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE, indexSettings, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING));
        assertThat(analyzingSuggestHolder.sepLabel, is(AnalyzingCompletionLookupProviderV1.SEP_LABEL));
        assertThat(analyzingSuggestHolder.payloadSep, is(AnalyzingCompletionLookupProviderV1.PAYLOAD_SEP));
        assertThat(analyzingSuggestHolder.endByte, is(AnalyzingCompletionLookupProviderV1.END_BYTE));
        dir.close();
    }

    @Test
    public void testProviderVersion2() throws IOException {
        AnalyzingCompletionLookupProvider currentProvider = new AnalyzingCompletionLookupProvider(true, false, true, true);

        RAMDirectory dir = new RAMDirectory();
        writeData(dir, currentProvider);

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = currentProvider.load(input);
        PostingsFormat format = new Elasticsearch090PostingsFormat();
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer());
        AnalyzingCompletionLookupProvider.AnalyzingSuggestHolder analyzingSuggestHolder = load.getAnalyzingSuggestHolder(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE, indexSettings, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING));
        assertThat(analyzingSuggestHolder.sepLabel, is(XAnalyzingSuggester.SEP_LABEL));
        assertThat(analyzingSuggestHolder.payloadSep, is(XAnalyzingSuggester.PAYLOAD_SEP));
        assertThat(analyzingSuggestHolder.endByte, is(XAnalyzingSuggester.END_BYTE));
        dir.close();
    }

    @Test
    public void testDuellCompletions() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException {
        final boolean preserveSeparators = getRandom().nextBoolean();
        final boolean preservePositionIncrements = getRandom().nextBoolean();
        final boolean usePayloads = getRandom().nextBoolean();
        final int options = preserveSeparators ? AnalyzingSuggester.PRESERVE_SEP : 0;

        XAnalyzingSuggester reference = new XAnalyzingSuggester(new StandardAnalyzer(), null, new StandardAnalyzer(), 
                options, 256, -1, preservePositionIncrements, null, false, 1, XAnalyzingSuggester.SEP_LABEL, XAnalyzingSuggester.PAYLOAD_SEP, XAnalyzingSuggester.END_BYTE, XAnalyzingSuggester.HOLE_CHARACTER);
        LineFileDocs docs = new LineFileDocs(getRandom());
        int num = scaledRandomIntBetween(150, 300);
        final String[] titles = new String[num];
        final long[] weights = new long[num];
        for (int i = 0; i < titles.length; i++) {
            Document nextDoc = docs.nextDoc();
            IndexableField field = nextDoc.getField("title");
            titles[i] = field.stringValue();
            weights[i] = between(0, 100);
           
        }
        docs.close();
        final InputIterator primaryIter = new InputIterator() {
            int index = 0;
            long currentWeight = -1;

            @Override
            public BytesRef next() throws IOException {
                if (index < titles.length) {
                    currentWeight = weights[index];
                    return new BytesRef(titles[index++]);
                }
                return null;
            }

            @Override
            public long weight() {
                return currentWeight;
            }

            @Override
            public BytesRef payload() {
                return null;
            }

            @Override
            public boolean hasPayloads() {
                return false;
            }

            @Override
            public Set<BytesRef> contexts() {
                return null;
            }

            @Override
            public boolean hasContexts() {
                return false;
            }

        };
        InputIterator iter;
        if (usePayloads) {
            iter = new InputIterator() {
                @Override
                public long weight() {
                    return primaryIter.weight();
                }

                @Override
                public BytesRef next() throws IOException {
                    return primaryIter.next();
                }

                @Override
                public BytesRef payload() {
                    return new BytesRef(Long.toString(weight()));
                }

                @Override
                public boolean hasPayloads() {
                    return true;
                }
                
                @Override
                public Set<BytesRef> contexts() {
                    return null;
                }

                @Override
                public boolean hasContexts() {
                    return false;
                }
            };
        } else {
            iter = primaryIter;
        }
        reference.build(iter);
        PostingsFormat provider = PostingsFormat.forName(Lucene.LATEST_POSTINGS_FORMAT);

        NamedAnalyzer namedAnalzyer = new NamedAnalyzer("foo", new StandardAnalyzer());
        final CompletionFieldMapper mapper = new CompletionFieldMapper(new Names("foo"), namedAnalzyer, namedAnalzyer, provider, null, usePayloads,
                preserveSeparators, preservePositionIncrements, Integer.MAX_VALUE, indexSettings, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING);
        Lookup buildAnalyzingLookup = buildAnalyzingLookup(mapper, titles, titles, weights);
        Field field = buildAnalyzingLookup.getClass().getDeclaredField("maxAnalyzedPathsForOneInput");
        field.setAccessible(true);
        Field refField = reference.getClass().getDeclaredField("maxAnalyzedPathsForOneInput");
        refField.setAccessible(true);
        assertThat(refField.get(reference), equalTo(field.get(buildAnalyzingLookup)));

        for (int i = 0; i < titles.length; i++) {
            int res = between(1, 10);
            final StringBuilder builder = new StringBuilder();
            SuggestUtils.analyze(namedAnalzyer.tokenStream("foo", titles[i]), new SuggestUtils.TokenConsumer() {
                @Override
                public void nextToken() throws IOException {
                    if (builder.length() == 0) {
                        builder.append(this.charTermAttr.toString());
                    }
                }
            });
            String firstTerm = builder.toString();
            String prefix = firstTerm.isEmpty() ? "" : firstTerm.substring(0, between(1, firstTerm.length()));
            List<LookupResult> refLookup = reference.lookup(prefix, false, res);
            List<LookupResult> lookup = buildAnalyzingLookup.lookup(prefix, false, res);
            assertThat(refLookup.toString(),lookup.size(), equalTo(refLookup.size()));
            for (int j = 0; j < refLookup.size(); j++) {
                assertThat(lookup.get(j).key, equalTo(refLookup.get(j).key));
                assertThat("prefix: " + prefix + " " + j + " -- missmatch cost: " + lookup.get(j).key + " - " + lookup.get(j).value + " | " + refLookup.get(j).key + " - " + refLookup.get(j).value ,
                        lookup.get(j).value, equalTo(refLookup.get(j).value));
                assertThat(lookup.get(j).payload, equalTo(refLookup.get(j).payload));
                if (usePayloads) {
                    assertThat(lookup.get(j).payload.utf8ToString(),  equalTo(Long.toString(lookup.get(j).value)));    
                }
            }
        }
    }

    public Lookup buildAnalyzingLookup(final CompletionFieldMapper mapper, String[] terms, String[] surfaces, long[] weights)
            throws IOException {
        RAMDirectory dir = new RAMDirectory();
        FilterCodec filterCodec = new FilterCodec("filtered", Codec.getDefault()) {
            @Override
            public PostingsFormat postingsFormat() {
                final PostingsFormat in = super.postingsFormat();
                return mapper.postingsFormat(in);
            }
        };
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(mapper.indexAnalyzer());

        indexWriterConfig.setCodec(filterCodec);
        IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
        for (int i = 0; i < weights.length; i++) {
            Document doc = new Document();
            BytesRef payload = mapper.buildPayload(new BytesRef(surfaces[i]), weights[i], new BytesRef(Long.toString(weights[i])));
            doc.add(mapper.getCompletionField(ContextMapping.EMPTY_CONTEXT, terms[i], payload));
            if (randomBoolean()) {
                writer.commit();
            }
            writer.addDocument(doc);
        }
        writer.commit();
        writer.forceMerge(1, true);
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer, true);
        assertThat(reader.leaves().size(), equalTo(1));
        assertThat(reader.leaves().get(0).reader().numDocs(), equalTo(weights.length));
        LeafReaderContext atomicReaderContext = reader.leaves().get(0);
        Terms luceneTerms = atomicReaderContext.reader().terms(mapper.name());
        Lookup lookup = ((Completion090PostingsFormat.CompletionTerms) luceneTerms).getLookup(mapper, new CompletionSuggestionContext(null));
        reader.close();
        writer.close();
        dir.close();
        return lookup;
    }
    @Test
    public void testNoDocs() throws IOException {
        AnalyzingCompletionLookupProvider provider = new AnalyzingCompletionLookupProvider(true, false, true, true);
        RAMDirectory dir = new RAMDirectory();
        IndexOutput output = dir.createOutput("foo.txt", IOContext.DEFAULT);
        FieldsConsumer consumer = provider.consumer(output);
        consumer.write(new Fields() {
            @Override
            public Iterator<String> iterator() {
                return Arrays.asList("foo").iterator();
            }

            @Override
            public Terms terms(String field) throws IOException {
                return null;
            }

            @Override
            public int size() {
                return 1;
            }
        });
        consumer.close();
        output.close();

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = provider.load(input);
        PostingsFormat format = new Elasticsearch090PostingsFormat();
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer());
        assertNull(load.getLookup(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE, indexSettings, AbstractFieldMapper.MultiFields.empty(), null, ContextMapping.EMPTY_MAPPING), new CompletionSuggestionContext(null)));
        dir.close();
    }

    // TODO ADD more unittests
    private void writeData(Directory dir, Completion090PostingsFormat.CompletionLookupProvider provider) throws IOException {
        IndexOutput output = dir.createOutput("foo.txt", IOContext.DEFAULT);
        FieldsConsumer consumer = provider.consumer(output);
        final List<TermPosAndPayload> terms = new ArrayList<>();
        terms.add(new TermPosAndPayload("foofightersgenerator", 256 - 2, provider.buildPayload(new BytesRef("Generator - Foo Fighters"), 9, new BytesRef("id:10"))));
        terms.add(new TermPosAndPayload("generator", 256 - 1, provider.buildPayload(new BytesRef("Generator - Foo Fighters"), 9, new BytesRef("id:10"))));
        Fields fields = new Fields() {
            @Override
            public Iterator<String> iterator() {
                return Arrays.asList("foo").iterator();
            }

            @Override
            public Terms terms(String field) throws IOException {
                if (field.equals("foo")) {
                    return new Terms() {
                        @Override
                        public TermsEnum iterator() throws IOException {
                            final Iterator<TermPosAndPayload> iterator = terms.iterator();
                            return new TermsEnum() {
                                private TermPosAndPayload current = null;
                                @Override
                                public SeekStatus seekCeil(BytesRef text) throws IOException {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public void seekExact(long ord) throws IOException {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public BytesRef term() throws IOException {
                                    return current == null ? null : current.term;
                                }

                                @Override
                                public long ord() throws IOException {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public int docFreq() throws IOException {
                                    return current == null ? 0 : 1;
                                }

                                @Override
                                public long totalTermFreq() throws IOException {
                                    throw new UnsupportedOperationException();
                                }

                                @Override
                                public PostingsEnum postings(Bits liveDocs, PostingsEnum reuse, int flags) throws IOException {
                                    final TermPosAndPayload data = current;
                                    return new PostingsEnum() {
                                        boolean done = false;
                                        @Override
                                        public int nextPosition() throws IOException {
                                            return current.pos;
                                        }

                                        @Override
                                        public int startOffset() throws IOException {
                                            return 0;
                                        }

                                        @Override
                                        public int endOffset() throws IOException {
                                            return 0;
                                        }

                                        @Override
                                        public BytesRef getPayload() throws IOException {
                                            return current.payload;
                                        }

                                        @Override
                                        public int freq() throws IOException {
                                            return 1;
                                        }

                                        @Override
                                        public int docID() {
                                            if (done) {
                                                return NO_MORE_DOCS;
                                            }
                                            return 0;
                                        }

                                        @Override
                                        public int nextDoc() throws IOException {
                                            if (done) {
                                                return NO_MORE_DOCS;
                                            }
                                            done = true;
                                            return 0;
                                        }

                                        @Override
                                        public int advance(int target) throws IOException {
                                            if (done) {
                                                return NO_MORE_DOCS;
                                            }
                                            done = true;
                                            return 0;
                                        }

                                        @Override
                                        public long cost() {
                                            return 0;
                                        }
                                    };
                                }

                                @Override
                                public BytesRef next() throws IOException {
                                    if (iterator.hasNext()) {
                                        current = iterator.next();
                                        return current.term;
                                    }
                                    current = null;
                                    return null;
                                }
                            };
                        }

                        @Override
                        public long size() throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public long getSumTotalTermFreq() throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public long getSumDocFreq() throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public int getDocCount() throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public boolean hasFreqs() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public boolean hasOffsets() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public boolean hasPositions() {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public boolean hasPayloads() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
                return null;
            }

            @Override
            public int size() {
                return 0;
            }
        };
        consumer.write(fields);
        consumer.close();
        output.close();

    }

    private static class TermPosAndPayload {
        final BytesRef term;
        final int pos;
        final BytesRef payload;


        private TermPosAndPayload(String term, int pos, BytesRef payload) {
            this.term = new BytesRef(term);
            this.pos = pos;
            this.payload = payload;
        }
    }
}
