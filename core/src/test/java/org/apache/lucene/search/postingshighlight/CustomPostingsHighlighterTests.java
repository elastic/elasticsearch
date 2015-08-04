/*
 * Licensed to Elasticsearch under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elasticsearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.lucene.search.postingshighlight;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.DefaultEncoder;
import org.apache.lucene.store.Directory;
import org.elasticsearch.search.highlight.HighlightUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;

public class CustomPostingsHighlighterTests extends ESTestCase {

    @Test
    public void testCustomPostingsHighlighter() throws Exception {

        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);

        //good position but only one match
        final String firstValue = "This is a test. Just a test1 highlighting from postings highlighter.";
        Field body = new Field("body", "", offsetsType);
        Document doc = new Document();
        doc.add(body);
        body.setStringValue(firstValue);

        //two matches, not the best snippet due to its length though
        final String secondValue = "This is the second highlighting value to perform highlighting on a longer text that gets scored lower.";
        Field body2 = new Field("body", "", offsetsType);
        doc.add(body2);
        body2.setStringValue(secondValue);

        //two matches and short, will be scored highest
        final String thirdValue = "This is highlighting the third short highlighting value.";
        Field body3 = new Field("body", "", offsetsType);
        doc.add(body3);
        body3.setStringValue(thirdValue);

        //one match, same as first but at the end, will be scored lower due to its position
        final String fourthValue = "Just a test4 highlighting from postings highlighter.";
        Field body4 = new Field("body", "", offsetsType);
        doc.add(body4);
        body4.setStringValue(fourthValue);

        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        String firstHlValue = "Just a test1 <b>highlighting</b> from postings highlighter.";
        String secondHlValue = "This is the second <b>highlighting</b> value to perform <b>highlighting</b> on a longer text that gets scored lower.";
        String thirdHlValue = "This is <b>highlighting</b> the third short <b>highlighting</b> value.";
        String fourthHlValue = "Just a test4 <b>highlighting</b> from postings highlighter.";

        IndexSearcher searcher = newSearcher(ir);
        Query query = new TermQuery(new Term("body", "highlighting"));

        TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));

        int docId = topDocs.scoreDocs[0].doc;

        String fieldValue = firstValue + HighlightUtils.PARAGRAPH_SEPARATOR + secondValue + HighlightUtils.PARAGRAPH_SEPARATOR + thirdValue + HighlightUtils.PARAGRAPH_SEPARATOR + fourthValue;

        CustomPostingsHighlighter highlighter = new CustomPostingsHighlighter(null, new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder()), fieldValue, false);
        Snippet[] snippets = highlighter.highlightField("body", query, searcher, docId, 5);

        assertThat(snippets.length, equalTo(4));

        assertThat(snippets[0].getText(), equalTo(firstHlValue));
        assertThat(snippets[1].getText(), equalTo(secondHlValue));
        assertThat(snippets[2].getText(), equalTo(thirdHlValue));
        assertThat(snippets[3].getText(), equalTo(fourthHlValue));

        ir.close();
        dir.close();
    }

    @Test
    public void testNoMatchSize() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random()));
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);

        FieldType offsetsType = new FieldType(TextField.TYPE_STORED);
        offsetsType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
        Field body = new Field("body", "", offsetsType);
        Field none = new Field("none", "", offsetsType);
        Document doc = new Document();
        doc.add(body);
        doc.add(none);

        String firstValue = "This is a test. Just a test highlighting from postings. Feel free to ignore.";
        body.setStringValue(firstValue);
        none.setStringValue(firstValue);
        iw.addDocument(doc);

        IndexReader ir = iw.getReader();
        iw.close();

        Query query = new TermQuery(new Term("none", "highlighting"));

        IndexSearcher searcher = newSearcher(ir);
        TopDocs topDocs = searcher.search(query, 10, Sort.INDEXORDER);
        assertThat(topDocs.totalHits, equalTo(1));
        int docId = topDocs.scoreDocs[0].doc;

        CustomPassageFormatter passageFormatter = new CustomPassageFormatter("<b>", "</b>", new DefaultEncoder());

        CustomPostingsHighlighter highlighter = new CustomPostingsHighlighter(null, passageFormatter, firstValue, false);
        Snippet[] snippets = highlighter.highlightField("body", query, searcher, docId, 5);
        assertThat(snippets.length, equalTo(0));

        highlighter = new CustomPostingsHighlighter(null, passageFormatter, firstValue, true);
        snippets = highlighter.highlightField("body", query, searcher, docId, 5);
        assertThat(snippets.length, equalTo(1));
        assertThat(snippets[0].getText(), equalTo("This is a test."));

        ir.close();
        dir.close();
    }
}
