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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Tests ported from Lucene.
 */
public class XBooleanFilterLuceneTests extends ElasticsearchTestCase {
    
    private Directory directory;
    private AtomicReader reader;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        directory = new RAMDirectory();
        IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(Lucene.VERSION, new WhitespaceAnalyzer(Lucene.VERSION)));

        //Add series of docs with filterable fields : acces rights, prices, dates and "in-stock" flags
        addDoc(writer, "admin guest", "010", "20040101", "Y");
        addDoc(writer, "guest", "020", "20040101", "Y");
        addDoc(writer, "guest", "020", "20050101", "Y");
        addDoc(writer, "admin", "020", "20050101", "Maybe");
        addDoc(writer, "admin guest", "030", "20050101", "N");
        writer.close();
        reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(directory));
        writer.close();
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        reader.close();
        directory.close();
    }

    private void addDoc(IndexWriter writer, String accessRights, String price, String date, String inStock) throws IOException {
        Document doc = new Document();
        doc.add(new TextField("accessRights", accessRights, Field.Store.YES));
        doc.add(new TextField("price", price, Field.Store.YES));
        doc.add(new TextField("date", date, Field.Store.YES));
        doc.add(new TextField("inStock", inStock, Field.Store.YES));
        writer.addDocument(doc);
    }

    private Filter getRangeFilter(String field, String lowerPrice, String upperPrice) {
        return TermRangeFilter.newStringRange(field, lowerPrice, upperPrice, true, true);
    }

    private Filter getTermsFilter(String field, String text) {
        return new TermsFilter(new Term(field, text));
    }

    private Filter getWrappedTermQuery(String field, String text) {
        return new QueryWrapperFilter(new TermQuery(new Term(field, text)));
    }

    private Filter getEmptyFilter() {
        return new Filter() {
            @Override
            public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
                return new FixedBitSet(context.reader().maxDoc());
            }
        };
    }

    private Filter getNullDISFilter() {
        return new Filter() {
            @Override
            public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
                return null;
            }
        };
    }

    private Filter getNullDISIFilter() {
        return new Filter() {
            @Override
            public DocIdSet getDocIdSet(AtomicReaderContext context, Bits acceptDocs) {
                return new DocIdSet() {
                    @Override
                    public DocIdSetIterator iterator() {
                        return null;
                    }

                    @Override
                    public boolean isCacheable() {
                        return true;
                    }
                };
            }
        };
    }

    private void tstFilterCard(String mes, int expected, Filter filt) throws Exception {
        int actual = 0;
        DocIdSet docIdSet = filt.getDocIdSet(reader.getContext(), reader.getLiveDocs());
        if (docIdSet != null) {
            DocIdSetIterator disi = docIdSet.iterator();
            if (disi != null) {
                while (disi.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    actual++;
                }
            }
        }
        assertThat(mes, actual, equalTo(expected));
    }

    @Test
    public void testShould() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("price", "030"), BooleanClause.Occur.SHOULD);
        tstFilterCard("Should retrieves only 1 doc", 1, booleanFilter);

        // same with a real DISI (no OpenBitSetIterator)
        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getWrappedTermQuery("price", "030"), BooleanClause.Occur.SHOULD);
        tstFilterCard("Should retrieves only 1 doc", 1, booleanFilter);
    }

    @Test
    public void testShoulds() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getRangeFilter("price", "010", "020"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getRangeFilter("price", "020", "030"), BooleanClause.Occur.SHOULD);
        tstFilterCard("Shoulds are Ored together", 5, booleanFilter);
    }

    @Test
    public void testShouldsAndMustNot() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getRangeFilter("price", "010", "020"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getRangeFilter("price", "020", "030"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getTermsFilter("inStock", "N"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("Shoulds Ored but AndNot", 4, booleanFilter);

        booleanFilter.add(getTermsFilter("inStock", "Maybe"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("Shoulds Ored but AndNots", 3, booleanFilter);

        // same with a real DISI (no OpenBitSetIterator)
        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getRangeFilter("price", "010", "020"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getRangeFilter("price", "020", "030"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getWrappedTermQuery("inStock", "N"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("Shoulds Ored but AndNot", 4, booleanFilter);

        booleanFilter.add(getWrappedTermQuery("inStock", "Maybe"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("Shoulds Ored but AndNots", 3, booleanFilter);
    }

    @Test
    public void testShouldsAndMust() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getRangeFilter("price", "010", "020"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getRangeFilter("price", "020", "030"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getTermsFilter("accessRights", "admin"), BooleanClause.Occur.MUST);
        tstFilterCard("Shoulds Ored but MUST", 3, booleanFilter);

        // same with a real DISI (no OpenBitSetIterator)
        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getRangeFilter("price", "010", "020"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getRangeFilter("price", "020", "030"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getWrappedTermQuery("accessRights", "admin"), BooleanClause.Occur.MUST);
        tstFilterCard("Shoulds Ored but MUST", 3, booleanFilter);
    }

    @Test
    public void testShouldsAndMusts() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getRangeFilter("price", "010", "020"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getRangeFilter("price", "020", "030"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getTermsFilter("accessRights", "admin"), BooleanClause.Occur.MUST);
        booleanFilter.add(getRangeFilter("date", "20040101", "20041231"), BooleanClause.Occur.MUST);
        tstFilterCard("Shoulds Ored but MUSTs ANDED", 1, booleanFilter);
    }

    @Test
    public void testShouldsAndMustsAndMustNot() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getRangeFilter("price", "030", "040"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getTermsFilter("accessRights", "admin"), BooleanClause.Occur.MUST);
        booleanFilter.add(getRangeFilter("date", "20050101", "20051231"), BooleanClause.Occur.MUST);
        booleanFilter.add(getTermsFilter("inStock", "N"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("Shoulds Ored but MUSTs ANDED and MustNot", 0, booleanFilter);

        // same with a real DISI (no OpenBitSetIterator)
        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getRangeFilter("price", "030", "040"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getWrappedTermQuery("accessRights", "admin"), BooleanClause.Occur.MUST);
        booleanFilter.add(getRangeFilter("date", "20050101", "20051231"), BooleanClause.Occur.MUST);
        booleanFilter.add(getWrappedTermQuery("inStock", "N"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("Shoulds Ored but MUSTs ANDED and MustNot", 0, booleanFilter);
    }

    @Test
    public void testJustMust() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("accessRights", "admin"), BooleanClause.Occur.MUST);
        tstFilterCard("MUST", 3, booleanFilter);

        // same with a real DISI (no OpenBitSetIterator)
        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getWrappedTermQuery("accessRights", "admin"), BooleanClause.Occur.MUST);
        tstFilterCard("MUST", 3, booleanFilter);
    }

    @Test
    public void testJustMustNot() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("inStock", "N"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("MUST_NOT", 4, booleanFilter);

        // same with a real DISI (no OpenBitSetIterator)
        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getWrappedTermQuery("inStock", "N"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("MUST_NOT", 4, booleanFilter);
    }

    @Test
    public void testMustAndMustNot() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("inStock", "N"), BooleanClause.Occur.MUST);
        booleanFilter.add(getTermsFilter("price", "030"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("MUST_NOT wins over MUST for same docs", 0, booleanFilter);

        // same with a real DISI (no OpenBitSetIterator)
        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getWrappedTermQuery("inStock", "N"), BooleanClause.Occur.MUST);
        booleanFilter.add(getWrappedTermQuery("price", "030"), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("MUST_NOT wins over MUST for same docs", 0, booleanFilter);
    }

    @Test
    public void testEmpty() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        tstFilterCard("empty XBooleanFilter returns no results", 0, booleanFilter);
    }

    @Test
    public void testCombinedNullDocIdSets() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("price", "030"), BooleanClause.Occur.MUST);
        booleanFilter.add(getNullDISFilter(), BooleanClause.Occur.MUST);
        tstFilterCard("A MUST filter that returns a null DIS should never return documents", 0, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("price", "030"), BooleanClause.Occur.MUST);
        booleanFilter.add(getNullDISIFilter(), BooleanClause.Occur.MUST);
        tstFilterCard("A MUST filter that returns a null DISI should never return documents", 0, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("price", "030"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getNullDISFilter(), BooleanClause.Occur.SHOULD);
        tstFilterCard("A SHOULD filter that returns a null DIS should be invisible", 1, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("price", "030"), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getNullDISIFilter(), BooleanClause.Occur.SHOULD);
        tstFilterCard("A SHOULD filter that returns a null DISI should be invisible", 1, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("price", "030"), BooleanClause.Occur.MUST);
        booleanFilter.add(getNullDISFilter(), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("A MUST_NOT filter that returns a null DIS should be invisible", 1, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("price", "030"), BooleanClause.Occur.MUST);
        booleanFilter.add(getNullDISIFilter(), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("A MUST_NOT filter that returns a null DISI should be invisible", 1, booleanFilter);
    }

    @Test
    public void testJustNullDocIdSets() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getNullDISFilter(), BooleanClause.Occur.MUST);
        tstFilterCard("A MUST filter that returns a null DIS should never return documents", 0, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getNullDISIFilter(), BooleanClause.Occur.MUST);
        tstFilterCard("A MUST filter that returns a null DISI should never return documents", 0, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getNullDISFilter(), BooleanClause.Occur.SHOULD);
        tstFilterCard("A single SHOULD filter that returns a null DIS should never return documents", 0, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getNullDISIFilter(), BooleanClause.Occur.SHOULD);
        tstFilterCard("A single SHOULD filter that returns a null DISI should never return documents", 0, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getNullDISFilter(), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("A single MUST_NOT filter that returns a null DIS should be invisible", 5, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getNullDISIFilter(), BooleanClause.Occur.MUST_NOT);
        tstFilterCard("A single MUST_NOT filter that returns a null DIS should be invisible", 5, booleanFilter);
    }

    @Test
    public void testNonMatchingShouldsAndMusts() throws Exception {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getEmptyFilter(), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getTermsFilter("accessRights", "admin"), BooleanClause.Occur.MUST);
        tstFilterCard(">0 shoulds with no matches should return no docs", 0, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getNullDISFilter(), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getTermsFilter("accessRights", "admin"), BooleanClause.Occur.MUST);
        tstFilterCard(">0 shoulds with no matches should return no docs", 0, booleanFilter);

        booleanFilter = new XBooleanFilter();
        booleanFilter.add(getNullDISIFilter(), BooleanClause.Occur.SHOULD);
        booleanFilter.add(getTermsFilter("accessRights", "admin"), BooleanClause.Occur.MUST);
        tstFilterCard(">0 shoulds with no matches should return no docs", 0, booleanFilter);
    }

    @Test
    public void testToStringOfBooleanFilterContainingTermsFilter() {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        booleanFilter.add(getTermsFilter("inStock", "N"), BooleanClause.Occur.MUST);
        booleanFilter.add(getTermsFilter("isFragile", "Y"), BooleanClause.Occur.MUST);

        assertThat("BooleanFilter(+inStock:N +isFragile:Y)", equalTo(booleanFilter.toString()));
    }

    @Test
    public void testToStringOfWrappedBooleanFilters() {
        XBooleanFilter orFilter = new XBooleanFilter();

        XBooleanFilter stockFilter = new XBooleanFilter();
        stockFilter.add(new FilterClause(getTermsFilter("inStock", "Y"), BooleanClause.Occur.MUST));
        stockFilter.add(new FilterClause(getTermsFilter("barCode", "12345678"), BooleanClause.Occur.MUST));

        orFilter.add(new FilterClause(stockFilter, BooleanClause.Occur.SHOULD));

        XBooleanFilter productPropertyFilter = new XBooleanFilter();
        productPropertyFilter.add(new FilterClause(getTermsFilter("isHeavy", "N"), BooleanClause.Occur.MUST));
        productPropertyFilter.add(new FilterClause(getTermsFilter("isDamaged", "Y"), BooleanClause.Occur.MUST));

        orFilter.add(new FilterClause(productPropertyFilter, BooleanClause.Occur.SHOULD));

        XBooleanFilter composedFilter = new XBooleanFilter();
        composedFilter.add(new FilterClause(orFilter, BooleanClause.Occur.MUST));

        assertThat(
                "BooleanFilter(+BooleanFilter(BooleanFilter(+inStock:Y +barCode:12345678) BooleanFilter(+isHeavy:N +isDamaged:Y)))",
                equalTo(composedFilter.toString())
        );
    }

}
