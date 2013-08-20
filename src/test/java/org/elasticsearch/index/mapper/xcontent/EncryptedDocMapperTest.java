package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.document.Document;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.attachment.AttachmentMapper;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.elasticsearch.common.io.Streams.copyToBytesFromClasspath;
import static org.elasticsearch.common.io.Streams.copyToStringFromClasspath;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Test for https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/18
 * Note that we have converted /org/elasticsearch/index/mapper/xcontent/testContentLength.txt
 * to a /org/elasticsearch/index/mapper/xcontent/encrypted.pdf with password `12345678`.
 */
public class EncryptedDocMapperTest {

    @Test
    public void testMultipleDocsEncryptedLast() throws IOException {
        DocumentMapperParser mapperParser = new DocumentMapperParser(new Index("test"), new AnalysisService(new Index("test")), null, null);
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multipledocs/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/htmlWithValidDateMeta.html");
        byte[] pdf = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/encrypted.pdf");

        BytesReference json = jsonBuilder()
                .startObject()
                    .field("_id", 1)
                    .field("file1", html)
                    .field("file2", pdf)
                .endObject().bytes();

        Document doc =  docMapper.parse(json).rootDoc();
        assertThat(doc.get(docMapper.mappers().smartName("file1").mapper().names().indexName()), containsString("World"));
        assertThat(doc.get(docMapper.mappers().smartName("file1.title").mapper().names().indexName()), equalTo("Hello"));
        assertThat(doc.get(docMapper.mappers().smartName("file1.author").mapper().names().indexName()), equalTo("kimchy"));
        assertThat(doc.get(docMapper.mappers().smartName("file1.keywords").mapper().names().indexName()), equalTo("elasticsearch,cool,bonsai"));
        assertThat(doc.get(docMapper.mappers().smartName("file1.content_type").mapper().names().indexName()), equalTo("text/html; charset=ISO-8859-1"));
        assertThat(doc.getField(docMapper.mappers().smartName("file1.content_length").mapper().names().indexName()).numericValue().longValue(), is(344L));

        assertThat(doc.get(docMapper.mappers().smartName("file2").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file2.title").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file2.author").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file2.keywords").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file2.content_type").mapper().names().indexName()), nullValue());
        assertThat(doc.getField(docMapper.mappers().smartName("file2.content_length").mapper().names().indexName()), nullValue());
    }

    @Test
    public void testMultipleDocsEncryptedFirst() throws IOException {
        DocumentMapperParser mapperParser = new DocumentMapperParser(new Index("test"), new AnalysisService(new Index("test")), null, null);
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multipledocs/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/htmlWithValidDateMeta.html");
        byte[] pdf = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/encrypted.pdf");

        BytesReference json = jsonBuilder()
                .startObject()
                .field("_id", 1)
                .field("file1", pdf)
                .field("file2", html)
                .endObject().bytes();

        Document doc =  docMapper.parse(json).rootDoc();
        assertThat(doc.get(docMapper.mappers().smartName("file1").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file1.title").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file1.author").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file1.keywords").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file1.content_type").mapper().names().indexName()), nullValue());
        assertThat(doc.getField(docMapper.mappers().smartName("file1.content_length").mapper().names().indexName()), nullValue());

        assertThat(doc.get(docMapper.mappers().smartName("file2").mapper().names().indexName()), containsString("World"));
        assertThat(doc.get(docMapper.mappers().smartName("file2.title").mapper().names().indexName()), equalTo("Hello"));
        assertThat(doc.get(docMapper.mappers().smartName("file2.author").mapper().names().indexName()), equalTo("kimchy"));
        assertThat(doc.get(docMapper.mappers().smartName("file2.keywords").mapper().names().indexName()), equalTo("elasticsearch,cool,bonsai"));
        assertThat(doc.get(docMapper.mappers().smartName("file2.content_type").mapper().names().indexName()), equalTo("text/html; charset=ISO-8859-1"));
        assertThat(doc.getField(docMapper.mappers().smartName("file2.content_length").mapper().names().indexName()).numericValue().longValue(), is(344L));
    }

    @Test(expectedExceptions = MapperParsingException.class)
    public void testMultipleDocsEncryptedNotIgnoringErrors() throws IOException {
        DocumentMapperParser mapperParser = new DocumentMapperParser(new Index("test"),
                ImmutableSettings.builder().put("index.mapping.attachment.ignore_errors", false).build(),
                new AnalysisService(new Index("test")), null, null);
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/multipledocs/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/htmlWithValidDateMeta.html");
        byte[] pdf = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/encrypted.pdf");

        BytesReference json = jsonBuilder()
                .startObject()
                .field("_id", 1)
                .field("file1", pdf)
                .field("file2", html)
                .endObject().bytes();

        Document doc =  docMapper.parse(json).rootDoc();
        assertThat(doc.get(docMapper.mappers().smartName("file1").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file1.title").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file1.author").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file1.keywords").mapper().names().indexName()), nullValue());
        assertThat(doc.get(docMapper.mappers().smartName("file1.content_type").mapper().names().indexName()), nullValue());
        assertThat(doc.getField(docMapper.mappers().smartName("file1.content_length").mapper().names().indexName()), nullValue());

        assertThat(doc.get(docMapper.mappers().smartName("file2").mapper().names().indexName()), containsString("World"));
        assertThat(doc.get(docMapper.mappers().smartName("file2.title").mapper().names().indexName()), equalTo("Hello"));
        assertThat(doc.get(docMapper.mappers().smartName("file2.author").mapper().names().indexName()), equalTo("kimchy"));
        assertThat(doc.get(docMapper.mappers().smartName("file2.keywords").mapper().names().indexName()), equalTo("elasticsearch,cool,bonsai"));
        assertThat(doc.get(docMapper.mappers().smartName("file2.content_type").mapper().names().indexName()), equalTo("text/html; charset=ISO-8859-1"));
        assertThat(doc.getField(docMapper.mappers().smartName("file2.content_length").mapper().names().indexName()).numericValue().longValue(), is(344L));
    }

}
