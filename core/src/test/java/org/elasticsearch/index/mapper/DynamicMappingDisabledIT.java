package org.elasticsearch.index.mapper;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

/**
 * Created by davidgalbraith on 12/17/15.
 */
public class DynamicMappingDisabledIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put("index.mapper.dynamic", "false")
            .build();
    }

    public void testDynamicDisabled() throws IOException {
        try {
            client().prepareIndex("index", "type", "1").setSource("foo", 3).get();
            fail("Indexing request should have failed");
        } catch (MapperParsingException e) {
            // expected
        }
    }

}
