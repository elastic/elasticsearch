/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.suggest;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.*;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.index.FieldInfo.DocValuesType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.search.spell.TermFreqIterator;
import org.apache.lucene.search.spell.TermFreqPayloadIterator;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.search.suggest.analyzing.AnalyzingSuggester;
import org.apache.lucene.search.suggest.analyzing.XAnalyzingSuggester;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LineFileDocs;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.codec.postingsformat.ElasticSearch090PostingsFormat;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.codec.postingsformat.PreBuiltPostingsFormatProvider;
import org.elasticsearch.index.mapper.FieldMapper.Names;
import org.elasticsearch.index.mapper.core.CompletionFieldMapper;
import org.elasticsearch.search.suggest.completion.AnalyzingCompletionLookupProvider;
import org.elasticsearch.search.suggest.completion.Completion090PostingsFormat;
import org.elasticsearch.search.suggest.completion.Completion090PostingsFormat.LookupFactory;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionContext;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class CompletionPostingsFormatTest extends ElasticsearchTestCase {

    @Test
    public void testCompletionPostingsFormat() throws IOException {
        AnalyzingCompletionLookupProvider provider = new AnalyzingCompletionLookupProvider(true, false, true, true);
        RAMDirectory dir = new RAMDirectory();
        IndexOutput output = dir.createOutput("foo.txt", IOContext.DEFAULT);
        FieldsConsumer consumer = provider.consumer(output);
        FieldInfo fieldInfo = new FieldInfo("foo", true, 1, false, true, true, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
                DocValuesType.SORTED, DocValuesType.BINARY, new HashMap<String, String>());
        TermsConsumer addField = consumer.addField(fieldInfo);

        PostingsConsumer postingsConsumer = addField.startTerm(new BytesRef("foofightersgenerator"));
        postingsConsumer.startDoc(0, 1);
        postingsConsumer.addPosition(256 - 2, provider.buildPayload(new BytesRef("Generator - Foo Fighters"), 9, new BytesRef("id:10")), 0,
                1);
        postingsConsumer.finishDoc();
        addField.finishTerm(new BytesRef("foofightersgenerator"), new TermStats(1, 1));
        addField.startTerm(new BytesRef("generator"));
        postingsConsumer.startDoc(0, 1);
        postingsConsumer.addPosition(256 - 1, provider.buildPayload(new BytesRef("Generator - Foo Fighters"), 9, new BytesRef("id:10")), 0,
                1);
        postingsConsumer.finishDoc();
        addField.finishTerm(new BytesRef("generator"), new TermStats(1, 1));
        addField.finish(1, 1, 1);
        consumer.close();
        output.close();

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = provider.load(input);
        PostingsFormatProvider format = new PreBuiltPostingsFormatProvider(new ElasticSearch090PostingsFormat());
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        Lookup lookup = load.getLookup(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE), new CompletionSuggestionContext(null));
        List<LookupResult> result = lookup.lookup("ge", false, 10);
        assertThat(result.get(0).key.toString(), equalTo("Generator - Foo Fighters"));
        assertThat(result.get(0).payload.utf8ToString(), equalTo("id:10"));
        dir.close();
    }

    @Test
    public void testDuellCompletions() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException,
            IllegalAccessException {
        final boolean preserveSeparators = getRandom().nextBoolean();
        final boolean preservePositionIncrements = getRandom().nextBoolean();
        final boolean usePayloads = getRandom().nextBoolean();
        final int options = preserveSeparators ? AnalyzingSuggester.PRESERVE_SEP : 0;

        XAnalyzingSuggester reference = new XAnalyzingSuggester(new StandardAnalyzer(TEST_VERSION_CURRENT), new StandardAnalyzer(
                TEST_VERSION_CURRENT), options, 256, -1, null, false, 1);
        reference.setPreservePositionIncrements(preservePositionIncrements);
        LineFileDocs docs = new LineFileDocs(getRandom());
        int num = atLeast(150);
        final String[] titles = new String[num];
        final long[] weights = new long[num];
        for (int i = 0; i < titles.length; i++) {
            Document nextDoc = docs.nextDoc();
            IndexableField field = nextDoc.getField("title");
            titles[i] = field.stringValue();
            weights[i] = between(0, 100);
           
        }
        docs.close();
        final TermFreqIterator primaryIter = new TermFreqIterator() {
            int index = 0;
            long currentWeight = -1;

            @Override
            public Comparator<BytesRef> getComparator() {
                return null;
            }

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

        };
        TermFreqIterator iter;
        if (usePayloads) {
            iter = new TermFreqPayloadIterator() {
                @Override
                public long weight() {
                    return primaryIter.weight();
                }

                @Override
                public Comparator<BytesRef> getComparator() {
                    return primaryIter.getComparator();
                }

                @Override
                public BytesRef next() throws IOException {
                    return primaryIter.next();
                }

                @Override
                public BytesRef payload() {
                    return new BytesRef(Long.toString(weight()));
                }
            };
        } else {
            iter = primaryIter;
        }
        reference.build(iter);
        PostingsFormatProvider provider = new PreBuiltPostingsFormatProvider(new ElasticSearch090PostingsFormat());

        NamedAnalyzer namedAnalzyer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        final CompletionFieldMapper mapper = new CompletionFieldMapper(new Names("foo"), namedAnalzyer, namedAnalzyer, provider, null, usePayloads,
                preserveSeparators, preservePositionIncrements, Integer.MAX_VALUE);
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
            public PostingsFormat postingsFormat() {
                return mapper.postingsFormatProvider().get();
            }
        };
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(TEST_VERSION_CURRENT, mapper.indexAnalyzer());

        indexWriterConfig.setCodec(filterCodec);
        IndexWriter writer = new IndexWriter(dir, indexWriterConfig);
        for (int i = 0; i < weights.length; i++) {
            Document doc = new Document();
            BytesRef payload = mapper.buildPayload(new BytesRef(surfaces[i]), weights[i], new BytesRef(Long.toString(weights[i])));
            doc.add(mapper.getCompletionField(terms[i], payload));
            if (randomBoolean()) {
                writer.commit();
            }
            writer.addDocument(doc);
        }
        writer.commit();
        writer.forceMerge(1);
        writer.commit();
        DirectoryReader reader = DirectoryReader.open(writer, true);
        assertThat(reader.leaves().size(), equalTo(1));
        assertThat(reader.leaves().get(0).reader().numDocs(), equalTo(weights.length));
        AtomicReaderContext atomicReaderContext = reader.leaves().get(0);
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
        FieldInfo fieldInfo = new FieldInfo("foo", true, 1, false, true, true, IndexOptions.DOCS_AND_FREQS_AND_POSITIONS,
                DocValuesType.SORTED, DocValuesType.BINARY, new HashMap<String, String>());
        TermsConsumer addField = consumer.addField(fieldInfo);
        addField.finish(0, 0, 0);
        consumer.close();
        output.close();

        IndexInput input = dir.openInput("foo.txt", IOContext.DEFAULT);
        LookupFactory load = provider.load(input);
        PostingsFormatProvider format = new PreBuiltPostingsFormatProvider(new ElasticSearch090PostingsFormat());
        NamedAnalyzer analyzer = new NamedAnalyzer("foo", new StandardAnalyzer(TEST_VERSION_CURRENT));
        assertNull(load.getLookup(new CompletionFieldMapper(new Names("foo"), analyzer, analyzer, format, null, true, true, true, Integer.MAX_VALUE), new CompletionSuggestionContext(null)));
        dir.close();
    }

    // TODO ADD more unittests
}
