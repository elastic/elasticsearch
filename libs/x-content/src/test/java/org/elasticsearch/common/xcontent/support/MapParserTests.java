package org.elasticsearch.common.xcontent.support;

import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

public class MapParserTests extends ESTestCase {

    private static final SimpleStruct STRUCT_A = new SimpleStruct(1, 0.1, "aaa");
    private static final SimpleStruct STRUCT_B = new SimpleStruct(2, 0.2, "bbb");
    private static final SimpleStruct STRUCT_C = new SimpleStruct(3, 0.3, "ccc");

    public void testReadGenericMap() throws IOException {
        String content = "{" +
            "\"c\": { \"i\": 3, \"d\": 0.3, \"s\": \"ccc\" }, " +
            "\"a\": { \"i\": 1, \"d\": 0.1, \"s\": \"aaa\" }, " +
            "\"b\": { \"i\": 2, \"d\": 0.2, \"s\": \"bbb\" }" +
            "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            Map<String, SimpleStruct> actualMap = MapParser.readGenericMap(parser, HashMap::new, SimpleStruct::fromXContent);
            // Verify map contents, ignore the iteration order.
            assertThat(actualMap, equalTo(Map.of("a", STRUCT_A, "b", STRUCT_B, "c", STRUCT_C)));
            assertThat(actualMap.values(), containsInAnyOrder(STRUCT_A, STRUCT_B, STRUCT_C));
            assertNull(parser.nextToken());
        }
    }

    public void testReadGenericMap_BackedByOrderedMap() throws IOException {
        String content = "{" +
            "\"c\": { \"i\": 3, \"d\": 0.3, \"s\": \"ccc\" }, " +
            "\"a\": { \"i\": 1, \"d\": 0.1, \"s\": \"aaa\" }, " +
            "\"b\": { \"i\": 2, \"d\": 0.2, \"s\": \"bbb\" }" +
            "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            Map<String, SimpleStruct> actualMap = MapParser.readGenericMap(parser, LinkedHashMap::new, SimpleStruct::fromXContent);
            // Verify map contents, ignore the iteration order.
            assertThat(actualMap, equalTo(Map.of("a", STRUCT_A, "b", STRUCT_B, "c", STRUCT_C)));
            // Verify that map's iteration order is the same as the order in which fields appear in JSON.
            assertThat(actualMap.values(), contains(STRUCT_C, STRUCT_A, STRUCT_B));
            assertNull(parser.nextToken());
        }
    }

    public void testReadGenericMap_Failure_UnparsableValue() throws IOException {
        String content = "{" +
            "\"a\": { \"i\": 1, \"d\": 0.1, \"s\": \"aaa\" }, " +
            "\"b\": { \"i\": 2, \"d\": 0.2, \"s\": 666 }, " +
            "\"c\": { \"i\": 3, \"d\": 0.3, \"s\": \"ccc\" }" +
            "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            XContentParseException exception = expectThrows(
                XContentParseException.class,
                () -> MapParser.readGenericMap(parser, HashMap::new, SimpleStruct::fromXContent));
            assertThat(exception, hasMessage(containsString("s doesn't support values of type: VALUE_NUMBER")));
        }
    }
}
