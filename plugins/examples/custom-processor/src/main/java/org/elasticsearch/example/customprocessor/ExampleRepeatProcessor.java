package org.elasticsearch.example.customprocessor;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

/**
 * Example of adding an ingest processor with a plugin.
 */
public class ExampleRepeatProcessor extends AbstractProcessor {
    public static final String TYPE = "repeat";
    public static final String FIELD_KEY_NAME = "field";

    private final String field;

    ExampleRepeatProcessor(String tag, String description, String field) {
        super(tag, description);
        this.field = field;
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object val = document.getFieldValue(field, Object.class, true);

        if (val instanceof String string) {
            String repeated = string.concat(string);
            document.setFieldValue(field, repeated);
        }
        return document;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public static class Factory implements Processor.Factory {

        @Override
        public ExampleRepeatProcessor create(
            Map<String, Processor.Factory> registry,
            String tag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) {
            String field = ConfigurationUtils.readStringProperty(TYPE, tag, config, FIELD_KEY_NAME);
            return new ExampleRepeatProcessor(tag, description, field);
        }
    }
}
