package org.elasticsearch.ingest.processor.meta;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheException;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocument.MetaData;

public class MetaDataProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        MetaDataProcessor.Factory factory = new MetaDataProcessor.Factory();
        Map<String, Object> config = new HashMap<>();
        for (MetaData metaData : MetaData.values()) {
            config.put(metaData.getFieldName(), randomBoolean() ? "static text" : "{{expression}}");
        }
        MetaDataProcessor processor = factory.create(config);
        assertThat(processor.getTemplates().size(), Matchers.equalTo(7));
        assertThat(processor.getTemplates().get(MetaData.INDEX), Matchers.notNullValue());
        assertThat(processor.getTemplates().get(MetaData.TIMESTAMP), Matchers.notNullValue());
        assertThat(processor.getTemplates().get(MetaData.ID), Matchers.notNullValue());
        assertThat(processor.getTemplates().get(MetaData.ROUTING), Matchers.notNullValue());
        assertThat(processor.getTemplates().get(MetaData.PARENT), Matchers.notNullValue());
        assertThat(processor.getTemplates().get(MetaData.TIMESTAMP), Matchers.notNullValue());
        assertThat(processor.getTemplates().get(MetaData.TTL), Matchers.notNullValue());
    }

    public void testCreateIllegalMetaData() throws Exception {
        MetaDataProcessor.Factory factory = new MetaDataProcessor.Factory();
        try {
            factory.create(Collections.singletonMap("_field", "text {{expression}}"));
            fail("exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), Matchers.equalTo("no valid metadata field name [_field]"));
        }
    }

    public void testCreateIllegalEmpty() throws Exception {
        MetaDataProcessor.Factory factory = new MetaDataProcessor.Factory();
        try {
            factory.create(Collections.emptyMap());
            fail("exception should have been thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), Matchers.equalTo("no meta fields specified"));
        }
    }

    public void testIlegalMustacheExpression() throws Exception {
        try {
            new MetaDataProcessor.Factory().create(Collections.singletonMap("_index", "text {{var"));
            fail("exception expected");
        } catch (MustacheException e) {
            assertThat(e.getMessage(), Matchers.equalTo("Improperly closed variable in :1"));
        }
    }

}
