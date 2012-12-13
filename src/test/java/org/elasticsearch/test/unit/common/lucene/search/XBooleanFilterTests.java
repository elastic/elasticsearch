package org.elasticsearch.test.unit.common.lucene.search;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.FieldCacheTermsFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.lucene.search.XBooleanFilter;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.search.BooleanClause.Occur.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 */
public class XBooleanFilterTests {

    private Directory directory;
    private AtomicReader reader;

    @BeforeClass
    public void setup() throws Exception {
        char[][] documentMatrix = new char[][] {
                {'a', 'b', 'c', 'd'},
                {'a', 'b', 'c', 'd'},
                {'a', 'a', 'a', 'a'}
        };

        List<Document> documents = new ArrayList<Document>(documentMatrix.length);
        for (char[] fields : documentMatrix) {
            Document document = new Document();
            for (int i = 0; i < fields.length; i++) {
                document.add(new StringField(Integer.toString(i), String.valueOf(fields[i]), Field.Store.NO));
            }
            documents.add(document);
        }
        directory = new RAMDirectory();
        IndexWriter w = new IndexWriter(directory, new IndexWriterConfig(Lucene.VERSION, new KeywordAnalyzer()));
        w.addDocuments(documents);
        w.close();
        reader = new SlowCompositeReaderWrapper(DirectoryReader.open(directory));
    }

    @AfterClass
    public void tearDown() throws Exception {
        reader.close();
        directory.close();
    }

    @Test
    public void testWithTwoClausesOfEachOccur_allFixedBitsetFilters() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<XBooleanFilter>();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, false),
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, false),
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, false)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, false),
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, false),
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, false)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, false),
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, false),
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, false)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    @Test
    public void testWithTwoClausesOfEachOccur_allBitsBasedFilters() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<XBooleanFilter>();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, true),
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, true),
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, true)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, true),
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, true),
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, true)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, true),
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, true),
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, true)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    @Test
    public void testWithTwoClausesOfEachOccur_allFilterTypes() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<XBooleanFilter>();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, false),
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, false),
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, false)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, false),
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, false),
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, false)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, true), newFilterClause(3, 'd', SHOULD, false),
                newFilterClause(4, 'e', MUST_NOT, true), newFilterClause(5, 'f', MUST_NOT, false),
                newFilterClause(0, 'a', MUST, true), newFilterClause(1, 'b', MUST, false)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }

        booleanFilters.clear();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, true),
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, true),
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, true)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, true),
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, true),
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, true)
        ));
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, false), newFilterClause(3, 'd', SHOULD, true),
                newFilterClause(4, 'e', MUST_NOT, false), newFilterClause(5, 'f', MUST_NOT, true),
                newFilterClause(0, 'a', MUST, false), newFilterClause(1, 'b', MUST, true)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    @Test
    public void testWithTwoClausesOfEachOccur_singleClauseOptimisation() throws Exception {
        List<XBooleanFilter> booleanFilters = new ArrayList<XBooleanFilter>();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'b', MUST, true)
        ));

        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }

        booleanFilters.clear();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(1, 'c', MUST_NOT, true)
        ));
        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(3));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(true));
        }

        booleanFilters.clear();
        booleanFilters.add(createBooleanFilter(
                newFilterClause(2, 'c', SHOULD, true)
        ));
        for (XBooleanFilter booleanFilter : booleanFilters) {
            FixedBitSet result = new FixedBitSet(reader.maxDoc());
            result.or(booleanFilter.getDocIdSet(reader.getContext(), reader.getLiveDocs()).iterator());
            assertThat(result.cardinality(), equalTo(2));
            assertThat(result.get(0), equalTo(true));
            assertThat(result.get(1), equalTo(true));
            assertThat(result.get(2), equalTo(false));
        }
    }

    private static FilterClause newFilterClause(int field, char character, BooleanClause.Occur occur, boolean slowerBitsBackedFilter) {
        Filter filter;
        if (slowerBitsBackedFilter) {
            filter = new FieldCacheTermsFilter(String.valueOf(field), String.valueOf(character));
        } else {
            Term term = new Term(String.valueOf(field), String.valueOf(character));
            filter = new TermFilter(term);
        }
        return new FilterClause(filter, occur);
    }

    private static XBooleanFilter createBooleanFilter(FilterClause... clauses) {
        XBooleanFilter booleanFilter = new XBooleanFilter();
        for (FilterClause clause : clauses) {
            booleanFilter.add(clause);
        }
        return booleanFilter;
    }

}
