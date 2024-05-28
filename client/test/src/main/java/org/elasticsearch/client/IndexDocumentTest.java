
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexDocumentTest extends ESRestTestCase {

    @Test
    public void testIndexDocument() throws IOException {
        try (RestHighLevelClient client = createClient()) {
            IndexRequest request = new IndexRequest("test-index");
            request.id("1");
            String jsonString = "{\"name\":\"John Doe\", \"age\":30, \"email\":\"john.doe@example.com\"}";
            request.source(jsonString, XContentType.JSON);

            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);

            assertNotNull(indexResponse);
            assertEquals("1", indexResponse.getId());
            assertEquals("test-index", indexResponse.getIndex());
        }
    }
}
