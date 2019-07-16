package org.elasticsearch.xpack.enrich;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

public class EnrichPolicyTests extends AbstractSerializingTestCase<EnrichPolicy> {

    @Override
    protected EnrichPolicy doParseInstance(XContentParser parser) throws IOException {
        return EnrichPolicy.fromXContent(parser);
    }

    @Override
    protected EnrichPolicy createTestInstance() {
        return randomEnrichPolicy(randomFrom(XContentType.values()));
    }

    @Override
    protected EnrichPolicy createXContextTestInstance(XContentType xContentType) {
        return randomEnrichPolicy(xContentType);
    }

    public static EnrichPolicy randomEnrichPolicy(XContentType xContentType) {
        return new EnrichPolicy(
            randomAlphaOfLength(4),
            randomFrom(Version.getDeclaredVersions(Version.class)),
            EnrichPolicyDefinitionTests.randomEnrichPolicyDefinition(xContentType)
        );
    }

    @Override
    protected Writeable.Reader<EnrichPolicy> instanceReader() {
        return EnrichPolicy::new;
    }

    @Override
    protected void assertEqualInstances(EnrichPolicy expectedInstance, EnrichPolicy newInstance) {
        assertNotSame(expectedInstance, newInstance);
        assertEquals(expectedInstance.getName(), newInstance.getName());
        assertEquals(expectedInstance.getVersionCreated(), newInstance.getVersionCreated());
        EnrichPolicyDefinitionTests.assertEqualPolicyDefinitions(expectedInstance.getDefinition(), newInstance.getDefinition());
    }
}
