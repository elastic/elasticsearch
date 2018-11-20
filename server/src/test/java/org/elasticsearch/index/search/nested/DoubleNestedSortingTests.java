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
package org.elasticsearch.index.search.nested;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.join.ToParentBlockJoinQuery;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DoubleNestedSortingTests extends AbstractNumberNestedSortingTestCase {

    @Override
    protected String getFieldDataType() {
        return "double";
    }

    @Override
    protected IndexFieldData.XFieldComparatorSource createFieldComparator(String fieldName, MultiValueMode sortMode,
                                                                                Object missingValue, Nested nested) {
        IndexNumericFieldData fieldData = getForField(fieldName);
        return new DoubleValuesComparatorSource(fieldData, missingValue, sortMode, nested);
    }

    @Override
    protected IndexableField createField(String name, int value) {
        return new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong(value));
    }

    @Override
    protected void assertAvgScoreMode(Query parentFilter, IndexSearcher searcher) throws IOException {
        MultiValueMode sortMode = MultiValueMode.AVG;
        Query childFilter = Queries.not(parentFilter);
        XFieldComparatorSource nestedComparatorSource = createFieldComparator("field2", sortMode, -127,
            createNested(searcher, parentFilter, childFilter));
        Query query = new ToParentBlockJoinQuery(new ConstantScoreQuery(childFilter),
            new QueryBitSetProducer(parentFilter), ScoreMode.None);
        Sort sort = new Sort(new SortField("field2", nestedComparatorSource));
        TopDocs topDocs = searcher.search(query, 5, sort);
        assertThat(topDocs.totalHits.value, equalTo(7L));
        assertThat(topDocs.scoreDocs.length, equalTo(5));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(11));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(15));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(19));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(3));
    }

}
