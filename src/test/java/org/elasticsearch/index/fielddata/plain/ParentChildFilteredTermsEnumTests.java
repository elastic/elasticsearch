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

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ElasticsearchLuceneTestCase;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.IsNull.notNullValue;

/**
 */
public class ParentChildFilteredTermsEnumTests extends ElasticsearchLuceneTestCase {

    @BeforeClass
    public static void before() {
        forceDefaultCodec();
    }

    @Test
    public void testSimple_twoFieldEachUniqueValue() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        for (int i = 1; i <= 10000; i++) {
            Document document = new Document();
            String fieldName = i % 2 == 0 ? "field1" : "field2";
            document.add(new StringField(fieldName, format(i), Field.Store.NO));
            indexWriter.addDocument(document);
        }

        IndexReader indexReader = DirectoryReader.open(indexWriter.w, false);
        TermsEnum[] compoundTermsEnums = new TermsEnum[]{
                new ParentChildIntersectTermsEnum(SlowCompositeReaderWrapper.wrap(indexReader), "field1", "field2")
        };
        for (TermsEnum termsEnum : compoundTermsEnums) {
            int expected = 0;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                ++expected;
                assertThat(term.utf8ToString(), equalTo(format(expected)));
                DocsEnum docsEnum = termsEnum.docs(null, null);
                assertThat(docsEnum, notNullValue());
                int docId = docsEnum.nextDoc();
                assertThat(docId, not(equalTo(-1)));
                assertThat(docId, not(equalTo(DocsEnum.NO_MORE_DOCS)));
                assertThat(docsEnum.nextDoc(), equalTo(DocsEnum.NO_MORE_DOCS));
            }
        }

        indexWriter.close();
        indexReader.close();
        directory.close();
    }

    @Test
    public void testDocument_twoFieldsEachSharingValues() throws Exception {
        Directory directory = newDirectory();
        RandomIndexWriter indexWriter = new RandomIndexWriter(random(), directory);
        for (int i = 1; i <= 1000; i++) {
            Document document = new Document();
            document.add(new StringField("field1", format(i), Field.Store.NO));
            indexWriter.addDocument(document);

            for (int j = 0; j < 10; j++) {
                document = new Document();
                document.add(new StringField("field2", format(i), Field.Store.NO));
                indexWriter.addDocument(document);
            }
        }

        IndexReader indexReader = DirectoryReader.open(indexWriter.w, false);
        TermsEnum[] compoundTermsEnums = new TermsEnum[]{
                new ParentChildIntersectTermsEnum(SlowCompositeReaderWrapper.wrap(indexReader), "field1", "field2")
        };
        for (TermsEnum termsEnum : compoundTermsEnums) {
            int expected = 0;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                ++expected;
                assertThat(term.utf8ToString(), equalTo(format(expected)));
                DocsEnum docsEnum = termsEnum.docs(null, null);
                assertThat(docsEnum, notNullValue());
                int numDocs = 0;
                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    numDocs++;
                }
                assertThat(numDocs, equalTo(11));
            }
        }


        indexWriter.close();
        indexReader.close();
        directory.close();
    }

    static String format(int i) {
        return String.format(Locale.ROOT, "%06d", i);
    }
}
