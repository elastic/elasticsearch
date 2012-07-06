package org.elasticsearch.test.unit.index.mapper.lucene;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.test.unit.index.mapper.MapperTests;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
@Test
public class DoubleIndexingDocTest {

    @Test
    public void testDoubleIndexingSameDoc() throws Exception {
        IndexWriter writer = new IndexWriter(new RAMDirectory(), new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties").endObject()
                .endObject().endObject().string();
        DocumentMapper mapper = MapperTests.newParser().parse(mapping);

        ParsedDocument doc = mapper.parse("type", "1", XContentFactory.jsonBuilder()
                .startObject()
                .field("field1", "value1")
                .field("field2", 1)
                .field("field3", 1.1)
                .field("field4", "2010-01-01")
                .startArray("field5").value(1).value(2).value(3).endArray()
                .endObject()
                .bytes());

        writer.addDocument(doc.rootDoc(), doc.analyzer());
        writer.addDocument(doc.rootDoc(), doc.analyzer());

        IndexReader reader = IndexReader.open(writer, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs topDocs = searcher.search(mapper.mappers().smartName("field1").mapper().fieldQuery("value1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field2").mapper().fieldQuery("1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field3").mapper().fieldQuery("1.1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field4").mapper().fieldQuery("2010-01-01", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field5").mapper().fieldQuery("1", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field5").mapper().fieldQuery("2", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));

        topDocs = searcher.search(mapper.mappers().smartName("field5").mapper().fieldQuery("3", null), 10);
        assertThat(topDocs.totalHits, equalTo(2));
    }
}
