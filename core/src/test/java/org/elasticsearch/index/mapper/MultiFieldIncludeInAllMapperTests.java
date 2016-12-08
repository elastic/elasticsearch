package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.MapperTestUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.IsEqual.equalTo;

/**
 * Created by makeyang on 2016/12/8.
 */
public class MultiFieldIncludeInAllMapperTests extends ESTestCase {
    public void testExceptionForCopyToInMultiFields() throws IOException {
        XContentBuilder mapping = createMappinmgWithIncludeInAllInMultiField();

        // first check that for newer versions we throw exception if copy_to is found withing multi field
        MapperService mapperService = MapperTestUtils.newMapperService(createTempDir(), Settings.EMPTY);
        try {
            mapperService.parse("type", new CompressedXContent(mapping.string()), true);
            fail("Parsing should throw an exception because the mapping contains a include_in_all in a multi field");
        } catch (MapperParsingException e) {
            assertThat(e.getMessage(), equalTo("include_in_all in multi fields is not allowed. Found the include_in_all in field [c] which is within a multi field."));
        }
    }

    private static XContentBuilder createMappinmgWithIncludeInAllInMultiField() throws IOException {
        XContentBuilder mapping = jsonBuilder();
        mapping.startObject()
                .startObject("type")
                .startObject("properties")
                .startObject("a")
                .field("type", "text")
                .endObject()
                .startObject("b")
                .field("type", "text")
                .startObject("fields")
                .startObject("c")
                .field("type", "text")
                .field("include_in_all", false)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        return mapping;
    }
}
