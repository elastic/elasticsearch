package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class NodeAttributesTests extends AbstractXContentTestCase<NodeAttributes> {

    public static NodeAttributes createRandom() {
        int numberOfAttributes = randomIntBetween(1, 10);
        Map<String, String> attributes = new HashMap<>(numberOfAttributes);
        for(int i = 0; i < numberOfAttributes; i++) {
            String val = randomAlphaOfLength(10);
            attributes.put("key-"+i, val);
        }
        return new NodeAttributes(randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            attributes);
    }

    @Override
    protected NodeAttributes createTestInstance() {
        return createRandom();
    }

    @Override
    protected NodeAttributes doParseInstance(XContentParser parser) throws IOException {
        return NodeAttributes.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
