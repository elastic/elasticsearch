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

package org.elasticsearch.common.lucene.index;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FreqTermsEnumTests extends ESTestCase {

    private String[] terms;
    private IndexWriter iw;
    private IndexReader reader;
    private Map<String, FreqHolder> referenceAll;
    private Map<String, FreqHolder> referenceNotDeleted;
    private Map<String, FreqHolder> referenceFilter;
    private Query filter;

    static class FreqHolder {
        int docFreq;
        long totalTermFreq;
    }


    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        referenceAll = new HashMap<>();
        referenceNotDeleted = new HashMap<>();
        referenceFilter = new HashMap<>();

        Directory dir = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(new KeywordAnalyzer()); // use keyword analyzer we rely on the stored field holding the exact term.
        if (frequently()) {
            // we don't want to do any merges, so we won't expunge deletes
            conf.setMergePolicy(NoMergePolicy.INSTANCE);
        }

        iw = new IndexWriter(dir, conf);
        terms = new String[scaledRandomIntBetween(10, 300)];
        for (int i = 0; i < terms.length; i++) {
            terms[i] = randomAlphaOfLength(5);
        }

        int numberOfDocs = scaledRandomIntBetween(30, 300);
        Document[] docs = new Document[numberOfDocs];
        for (int i = 0; i < numberOfDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            docs[i] = doc;
            for (String term : terms) {
                if (randomBoolean()) {
                    continue;
                }
                int freq = randomIntBetween(1, 3);
                for (int j = 0; j < freq; j++) {
                    doc.add(new TextField("field", term, Field.Store.YES));
                }
            }
        }

        // add all docs

        for (int i = 0; i < docs.length; i++) {
            Document doc = docs[i];
            iw.addDocument(doc);
            if (rarely()) {
                iw.commit();
            }
        }

        Set<String> deletedIds = new HashSet<>();
        for (int i = 0; i < docs.length; i++) {
            Document doc = docs[i];
            if (randomInt(5) == 2) {
                Term idTerm = new Term("id", doc.getField("id").stringValue());
                deletedIds.add(idTerm.text());
                iw.deleteDocuments(idTerm);
            }
        }

        for (String term : terms) {
            referenceAll.put(term, new FreqHolder());
            referenceFilter.put(term, new FreqHolder());
            referenceNotDeleted.put(term, new FreqHolder());
        }

        // now go over each doc, build the relevant references and filter
        reader = DirectoryReader.open(iw);
        List<BytesRef> filterTerms = new ArrayList<>();
        for (int docId = 0; docId < reader.maxDoc(); docId++) {
            Document doc = reader.document(docId);
            addFreqs(doc, referenceAll);
            if (!deletedIds.contains(doc.getField("id").stringValue())) {
                addFreqs(doc, referenceNotDeleted);
                if (randomBoolean()) {
                    filterTerms.add(new BytesRef(doc.getField("id").stringValue()));
                    addFreqs(doc, referenceFilter);
                }
            }
        }
        filter = new TermInSetQuery("id",filterTerms);
    }

    private void addFreqs(Document doc, Map<String, FreqHolder> reference) {
        Set<String> addedDocFreq = new HashSet<>();
        for (IndexableField field : doc.getFields("field")) {
            String term = field.stringValue();
            FreqHolder freqHolder = reference.get(term);
            if (!addedDocFreq.contains(term)) {
                freqHolder.docFreq++;
                addedDocFreq.add(term);
            }
            freqHolder.totalTermFreq++;
        }
    }

    @After
    @Override
    public void tearDown() throws Exception {
        IOUtils.close(reader, iw, iw.getDirectory());
        super.tearDown();
    }

    public void testAllFreqs() throws Exception {
        assertAgainstReference(true, true, null, referenceAll);
        assertAgainstReference(true, false, null, referenceAll);
        assertAgainstReference(false, true, null, referenceAll);
    }

    public void testNonDeletedFreqs() throws Exception {
        assertAgainstReference(true, true, Queries.newMatchAllQuery(), referenceNotDeleted);
        assertAgainstReference(true, false, Queries.newMatchAllQuery(), referenceNotDeleted);
        assertAgainstReference(false, true, Queries.newMatchAllQuery(), referenceNotDeleted);
    }

    public void testFilterFreqs() throws Exception {
        assertAgainstReference(true, true, filter, referenceFilter);
        assertAgainstReference(true, false, filter, referenceFilter);
        assertAgainstReference(false, true, filter, referenceFilter);
    }

    private void assertAgainstReference(boolean docFreq, boolean totalTermFreq, Query filter, Map<String, FreqHolder> reference) throws Exception {
        FreqTermsEnum freqTermsEnum = new FreqTermsEnum(reader, "field", docFreq, totalTermFreq, filter, BigArrays.NON_RECYCLING_INSTANCE);
        assertAgainstReference(freqTermsEnum, reference, docFreq, totalTermFreq);
    }

    private void assertAgainstReference(FreqTermsEnum termsEnum, Map<String, FreqHolder> reference, boolean docFreq, boolean totalTermFreq) throws Exception {
        int cycles = randomIntBetween(1, 5);
        for (int i = 0; i < cycles; i++) {
            List<String> terms = new ArrayList<>(Arrays.asList(this.terms));

           Collections.shuffle(terms, random());
            for (String term : terms) {
                if (!termsEnum.seekExact(new BytesRef(term))) {
                    assertThat("term : " + term, reference.get(term).docFreq, is(0));
                    continue;
                }
                if (docFreq) {
                    assertThat("cycle " + i + ", term " + term + ", docFreq", termsEnum.docFreq(), equalTo(reference.get(term).docFreq));
                }
                if (totalTermFreq) {
                    assertThat("cycle " + i + ", term " + term + ", totalTermFreq", termsEnum.totalTermFreq(), equalTo(reference.get(term).totalTermFreq));
                }
            }
        }
    }
}
