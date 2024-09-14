/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class FloatNestedSortingTests extends DoubleNestedSortingTests {

    @Override
    protected String getFieldDataType() {
        return "float";
    }

    @Override
    protected IndexFieldData.XFieldComparatorSource createFieldComparator(
        String fieldName,
        MultiValueMode sortMode,
        Object missingValue,
        Nested nested
    ) {
        IndexNumericFieldData fieldData = getForField(fieldName);
        return new FloatValuesComparatorSource(fieldData, missingValue, sortMode, nested);
    }

    @Override
    protected IndexableField createField(String name, int value) {
        return new SortedNumericDocValuesField(name, NumericUtils.floatToSortableInt(value));
    }

    protected void assertAvgScoreMode(
        Query parentFilter,
        IndexSearcher searcher,
        IndexFieldData.XFieldComparatorSource innerFieldComparator
    ) throws IOException {
        MultiValueMode sortMode = MultiValueMode.AVG;
        Query childFilter = Queries.not(parentFilter);
        XFieldComparatorSource nestedComparatorSource = createFieldComparator(
            "field2",
            sortMode,
            -127,
            createNested(searcher, parentFilter, childFilter)
        );
        Query query = new ToParentBlockJoinQuery(
            new ConstantScoreQuery(childFilter),
            new QueryBitSetProducer(parentFilter),
            ScoreMode.None
        );
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
