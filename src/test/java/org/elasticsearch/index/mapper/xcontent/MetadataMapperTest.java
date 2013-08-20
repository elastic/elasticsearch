package org.elasticsearch.index.mapper.xcontent;

import org.apache.lucene.document.Document;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
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
 * Test for https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/38
 */
public class MetadataMapperTest {

    protected void checkMeta(String filename, Settings settings, Long expectedDate, Long expectedLength) throws IOException {
        DocumentMapperParser mapperParser = new DocumentMapperParser(new Index("test"), settings, new AnalysisService(new Index("test")), null, null);
        mapperParser.putTypeParser(AttachmentMapper.CONTENT_TYPE, new AttachmentMapper.TypeParser());

        String mapping = copyToStringFromClasspath("/org/elasticsearch/index/mapper/xcontent/test-mapping.json");
        DocumentMapper docMapper = mapperParser.parse(mapping);
        byte[] html = copyToBytesFromClasspath("/org/elasticsearch/index/mapper/xcontent/" + filename);

        BytesReference json = jsonBuilder()
                .startObject()
                    .field("_id", 1)
                    .startObject("file")
                        .field("_name", filename)
                        .field("content", html)
                    .endObject()
                .endObject().bytes();

        Document doc =  docMapper.parse(json).rootDoc();
        assertThat(doc.get(docMapper.mappers().smartName("file").mapper().names().indexName()), containsString("World"));
        assertThat(doc.get(docMapper.mappers().smartName("file.name").mapper().names().indexName()), equalTo(filename));
        if (expectedDate == null) {
            assertThat(doc.getField(docMapper.mappers().smartName("file.date").mapper().names().indexName()), nullValue());
        } else {
            assertThat(doc.getField(docMapper.mappers().smartName("file.date").mapper().names().indexName()).numericValue().longValue(), is(expectedDate));
        }
        assertThat(doc.get(docMapper.mappers().smartName("file.title").mapper().names().indexName()), equalTo("Hello"));
        assertThat(doc.get(docMapper.mappers().smartName("file.author").mapper().names().indexName()), equalTo("kimchy"));
        assertThat(doc.get(docMapper.mappers().smartName("file.keywords").mapper().names().indexName()), equalTo("elasticsearch,cool,bonsai"));
        assertThat(doc.get(docMapper.mappers().smartName("file.content_type").mapper().names().indexName()), equalTo("text/html; charset=ISO-8859-1"));
        assertThat(doc.getField(docMapper.mappers().smartName("file.content_length").mapper().names().indexName()).numericValue().longValue(), is(expectedLength));
    }

    @Test
    public void testIgnoreWithoutDate() throws Exception {
        checkMeta("htmlWithoutDateMeta.html", ImmutableSettings.builder().build(), null, 300L);
    }

    @Test
    public void testIgnoreWithEmptyDate() throws Exception {
        checkMeta("htmlWithEmptyDateMeta.html", ImmutableSettings.builder().build(), null, 334L);
    }

    @Test
    public void testIgnoreWithCorrectDate() throws Exception {
        checkMeta("htmlWithValidDateMeta.html", ImmutableSettings.builder().build(), 1354233600000L, 344L);
    }

    @Test
    public void testWithoutDate() throws Exception {
        checkMeta("htmlWithoutDateMeta.html", ImmutableSettings.builder().put("index.mapping.attachment.ignore_errors", false).build(), null, 300L);
    }

    @Test(expectedExceptions = MapperParsingException.class)
    public void testWithEmptyDate() throws Exception {
        checkMeta("htmlWithEmptyDateMeta.html", ImmutableSettings.builder().put("index.mapping.attachment.ignore_errors", false).build(), null, null);
    }

    @Test
    public void testWithCorrectDate() throws Exception {
        checkMeta("htmlWithValidDateMeta.html", ImmutableSettings.builder().put("index.mapping.attachment.ignore_errors", false).build(), 1354233600000L, 344L);
    }
}
