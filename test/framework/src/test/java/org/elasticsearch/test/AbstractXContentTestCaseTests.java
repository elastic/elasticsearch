package org.elasticsearch.test;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class AbstractXContentTestCaseTests extends ESTestCase {

    public void testInsertRandomFieldsAndShuffle() throws Exception {
        TestInstance t = new TestInstance();
        BytesReference insertRandomFieldsAndShuffle = RandomizedContext.current().runWithPrivateRandomness(1,
                () -> AbstractXContentTestCase.insertRandomFieldsAndShuffle(t, XContentType.JSON, true, new String[] {}, null,
                        this::createParser, ToXContent.EMPTY_PARAMS));
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), insertRandomFieldsAndShuffle)) {
            Map<String, Object> mapOrdered = parser.mapOrdered();
            assertThat(mapOrdered.size(), equalTo(2));
            assertThat(mapOrdered.keySet().iterator().next(), not(equalTo("field")));
        }
    }

    private class TestInstance implements ToXContentObject {

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("field", 1);
            }
            builder.endObject();
            return builder;
        }

    }

}