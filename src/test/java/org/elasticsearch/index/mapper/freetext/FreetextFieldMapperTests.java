package org.elasticsearch.index.mapper.freetext;

import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.index.mapper.core.FreetextFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.instanceOf;

/**
 *
 */
public class FreetextFieldMapperTests extends ElasticsearchTestCase {

    @Test
    public void testDefaultConfiguration() throws IOException {
        String mapping = jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("freetext")
                .field("type", "freetext")
                .endObject().endObject()
                .endObject().endObject().string();

        DocumentMapper defaultMapper = MapperTestUtils.newParser().parse(mapping);

        FieldMapper fieldMapper = defaultMapper.mappers().name("freetext").mapper();
        assertThat(fieldMapper, instanceOf(FreetextFieldMapper.class));

        FreetextFieldMapper freetextFieldMapper = (FreetextFieldMapper) fieldMapper;
        //assertThat(completionFieldMapper.isStoringPayloads(), is(false));
    }

}
