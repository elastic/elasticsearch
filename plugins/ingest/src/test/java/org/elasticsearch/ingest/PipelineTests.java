package org.elasticsearch.ingest;

import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.ingest.processor.set.SetProcessor;
import org.elasticsearch.ingest.processor.remove.RemoveProcessor;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PipelineTests extends ESTestCase {

    public void testProcessorSettingsRemainUntouched() throws Exception {
        Map<String, Object> subField = new HashMap<>();
        subField.put("_subfield", "value");
        Map<String, Object> fieldSettings = new HashMap<>();
        fieldSettings.put("_field", subField);
        Map<String, Object> addSettings = new HashMap<>();
        addSettings.put("fields", fieldSettings);
        Map<String, Object> removeSettings = new HashMap<>();
        removeSettings.put("fields", Collections.singletonList("_field._subfield"));
        Pipeline pipeline = createPipeline(processorConfig(SetProcessor.TYPE, addSettings), processorConfig(RemoveProcessor.TYPE, removeSettings));

        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", new HashMap<>());
        pipeline.execute(ingestDocument);

        assertThat(ingestDocument.getSource().get("_field"), Matchers.notNullValue());
        assertThat(((Map) ingestDocument.getSource().get("_field")).get("_subfield"), Matchers.nullValue());
        assertThat(((Map) fieldSettings.get("_field")).get("_subfield"), Matchers.equalTo("value"));
    }

    private Pipeline createPipeline(Map<String, Object>... processorConfigs) throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put("processors", Arrays.asList(processorConfigs));
        Map<String, Processor.Factory> factoryRegistry = new HashMap<>();
        factoryRegistry.put(SetProcessor.TYPE, new SetProcessor.Factory());
        factoryRegistry.put(RemoveProcessor.TYPE, new RemoveProcessor.Factory());
        Pipeline.Factory factory = new Pipeline.Factory();
        return factory.create("_id", config, factoryRegistry);
    }

    private Map<String, Object> processorConfig(String type, Map<String, Object> settings) {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put(type, settings);
        return processorConfig;
    }

}
