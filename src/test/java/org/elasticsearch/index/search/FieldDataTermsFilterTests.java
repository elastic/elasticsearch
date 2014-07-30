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

package org.elasticsearch.index.search;

import com.carrotsearch.hppc.DoubleOpenHashSet;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.ObjectOpenHashSet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.mapper.ContentPath;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.core.DoubleFieldMapper;
import org.elasticsearch.index.mapper.core.LongFieldMapper;
import org.elasticsearch.index.mapper.core.NumberFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ElasticsearchSingleNodeTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 */
public class FieldDataTermsFilterTests extends ElasticsearchSingleNodeTest {

    protected QueryParseContext parseContext;
    protected IndexFieldDataService ifdService;
    protected IndexWriter writer;
    protected AtomicReader reader;
    protected StringFieldMapper strMapper;
    protected LongFieldMapper lngMapper;
    protected DoubleFieldMapper dblMapper;

    @Before
    public void setup() throws Exception {
        super.setUp();

        // create index and fielddata service
        IndexService indexService = createIndex("test", ImmutableSettings.builder().put("index.fielddata.cache", "none").build());
        Settings settings = indexService.settingsService().getSettings();
        ifdService = indexService.injector().getInstance(IndexFieldDataService.class);
        IndexQueryParserService parserService = indexService.queryParserService();
        parseContext = new QueryParseContext(indexService.index(), parserService);
        writer = new IndexWriter(new RAMDirectory(),
                new IndexWriterConfig(Lucene.VERSION, new StandardAnalyzer(Lucene.VERSION)));

        // setup field mappers
        strMapper = new StringFieldMapper.Builder("str_value")
                .build(new Mapper.BuilderContext(settings, new ContentPath(1)));

        lngMapper = new LongFieldMapper.Builder("lng_value")
                .build(new Mapper.BuilderContext(settings, new ContentPath(1)));

        dblMapper = new DoubleFieldMapper.Builder("dbl_value")
                .build(new Mapper.BuilderContext(settings, new ContentPath(1)));

        int numDocs = 10;
        for (int i = 0; i < numDocs; i++) {
            Document d = new Document();
            d.add(new StringField(strMapper.names().indexName(), "str" + i, Field.Store.NO));
            d.add(new LongField(lngMapper.names().indexName(), i, Field.Store.NO));
            d.add(new DoubleField(dblMapper.names().indexName(), Double.valueOf(i), Field.Store.NO));
            writer.addDocument(d);
        }

        reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(writer, true));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        reader.close();
        writer.close();
        ifdService.clear();
        SearchContext.removeCurrent();
    }

    protected <IFD extends IndexFieldData> IFD getFieldData(FieldMapper fieldMapper) {
        return ifdService.getForField(fieldMapper);
    }

    protected <IFD extends IndexNumericFieldData> IFD getFieldData(NumberFieldMapper fieldMapper) {
        return ifdService.getForField(fieldMapper);
    }

    @Test
    public void testBytes() throws Exception {
        List<Integer> docs = Arrays.asList(1, 5, 7);

        ObjectOpenHashSet<BytesRef> hTerms = new ObjectOpenHashSet<>();
        List<BytesRef> cTerms = new ArrayList<>(docs.size());
        for (int i = 0; i < docs.size(); i++) {
            BytesRef term = new BytesRef("str" + docs.get(i));
            hTerms.add(term);
            cTerms.add(term);
        }

        FieldDataTermsFilter hFilter = FieldDataTermsFilter.newBytes(getFieldData(strMapper), hTerms);

        int size = reader.maxDoc();
        FixedBitSet result = new FixedBitSet(size);

        result.clear(0, size);
        assertThat(result.cardinality(), equalTo(0));
        result.or(hFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(docs.size()));
        for (int i = 0; i < reader.maxDoc(); i++) {
            assertThat(result.get(i), equalTo(docs.contains(i)));
        }

        // filter from mapper
        result.clear(0, size);
        assertThat(result.cardinality(), equalTo(0));
        result.or(strMapper.fieldDataTermsFilter(cTerms, parseContext)
                .getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(docs.size()));
        for (int i = 0; i < reader.maxDoc(); i++) {
            assertThat(result.get(i), equalTo(docs.contains(i)));
        }

        result.clear(0, size);
        assertThat(result.cardinality(), equalTo(0));

        // filter on a numeric field using BytesRef terms
        // should not match any docs
        hFilter = FieldDataTermsFilter.newBytes(getFieldData(lngMapper), hTerms);
        result.or(hFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(0));

        // filter on a numeric field using BytesRef terms
        // should not match any docs
        hFilter = FieldDataTermsFilter.newBytes(getFieldData(dblMapper), hTerms);
        result.or(hFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(0));
    }

    @Test
    public void testLongs() throws Exception {
        List<Integer> docs = Arrays.asList(1, 5, 7);

        LongOpenHashSet hTerms = new LongOpenHashSet();
        List<Long> cTerms = new ArrayList<>(docs.size());
        for (int i = 0; i < docs.size(); i++) {
            long term = docs.get(i).longValue();
            hTerms.add(term);
            cTerms.add(term);
        }

        FieldDataTermsFilter hFilter = FieldDataTermsFilter.newLongs(getFieldData(lngMapper), hTerms);

        int size = reader.maxDoc();
        FixedBitSet result = new FixedBitSet(size);

        result.clear(0, size);
        assertThat(result.cardinality(), equalTo(0));
        result.or(hFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(docs.size()));
        for (int i = 0; i < reader.maxDoc(); i++) {
            assertThat(result.get(i), equalTo(docs.contains(i)));
        }

        // filter from mapper
        result.clear(0, size);
        assertThat(result.cardinality(), equalTo(0));
        result.or(lngMapper.fieldDataTermsFilter(cTerms, parseContext)
                .getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(docs.size()));
        for (int i = 0; i < reader.maxDoc(); i++) {
            assertThat(result.get(i), equalTo(docs.contains(i)));
        }

        hFilter = FieldDataTermsFilter.newLongs(getFieldData(dblMapper), hTerms);
        assertNull(hFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()));
    }

    @Test
    public void testDoubles() throws Exception {
        List<Integer> docs = Arrays.asList(1, 5, 7);

        DoubleOpenHashSet hTerms = new DoubleOpenHashSet();
        List<Double> cTerms = new ArrayList<>(docs.size());
        for (int i = 0; i < docs.size(); i++) {
            double term = Double.valueOf(docs.get(i));
            hTerms.add(term);
            cTerms.add(term);
        }

        FieldDataTermsFilter hFilter = FieldDataTermsFilter.newDoubles(getFieldData(dblMapper), hTerms);

        int size = reader.maxDoc();
        FixedBitSet result = new FixedBitSet(size);

        result.clear(0, size);
        assertThat(result.cardinality(), equalTo(0));
        result.or(hFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(docs.size()));
        for (int i = 0; i < reader.maxDoc(); i++) {
            assertThat(result.get(i), equalTo(docs.contains(i)));
        }

        // filter from mapper
        result.clear(0, size);
        assertThat(result.cardinality(), equalTo(0));
        result.or(dblMapper.fieldDataTermsFilter(cTerms, parseContext)
                .getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
        assertThat(result.cardinality(), equalTo(docs.size()));
        for (int i = 0; i < reader.maxDoc(); i++) {
            assertThat(result.get(i), equalTo(docs.contains(i)));
        }

        hFilter = FieldDataTermsFilter.newDoubles(getFieldData(lngMapper), hTerms);
        assertNull(hFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()));
    }

    @Test
    public void testNoTerms() throws Exception {
        FieldDataTermsFilter hFilterBytes = FieldDataTermsFilter.newBytes(getFieldData(strMapper), new ObjectOpenHashSet<BytesRef>());
        FieldDataTermsFilter hFilterLongs = FieldDataTermsFilter.newLongs(getFieldData(lngMapper), new LongOpenHashSet());
        FieldDataTermsFilter hFilterDoubles = FieldDataTermsFilter.newDoubles(getFieldData(dblMapper), new DoubleOpenHashSet());
        assertNull(hFilterBytes.getDocIdSet(reader.getContext(), reader.getLiveDocs()));
        assertNull(hFilterLongs.getDocIdSet(reader.getContext(), reader.getLiveDocs()));
        assertNull(hFilterDoubles.getDocIdSet(reader.getContext(), reader.getLiveDocs()));
    }
}
