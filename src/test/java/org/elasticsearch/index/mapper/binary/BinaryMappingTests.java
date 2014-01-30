package org.elasticsearch.index.mapper.binary;

import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperTestUtils;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

/**
 */
public class BinaryMappingTests extends ElasticsearchTestCase {

    @Test
    public void testDefaultMapping() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type")
                .startObject("properties")
                .startObject("field")
                .field("type", "binary")
                .endObject()
                .endObject()
                .endObject().endObject().string();

        DocumentMapper mapper = MapperTestUtils.newParser().parse(mapping);

        FieldMapper fieldMapper = mapper.mappers().smartNameFieldMapper("field");
        assertThat(fieldMapper, instanceOf(BinaryFieldMapper.class));
        assertThat(fieldMapper.fieldType().stored(), equalTo(false));
    }

}
