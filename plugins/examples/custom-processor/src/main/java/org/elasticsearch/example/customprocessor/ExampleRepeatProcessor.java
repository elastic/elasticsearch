package org.elasticsearch.example.customprocessor;

import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.util.Map;

/**
 * Example of adding an ingest processor with a plugin.
 */
public class ExampleRepeatProcessor extends AbstractProcessor {
    public static final String TYPE = "repeat";
    public static final String FILED_TO_REPEAT = "toRepeat";

    ExampleRepeatProcessor(String tag, String description) {
        super(tag, description);
    }

    @Override
    public IngestDocument execute(IngestDocument document) {
        Object val = document.getFieldValue(FILED_TO_REPEAT, Object.class, true);

        if (val instanceof String string) {
            String repeated = string.concat(string);
            document.setFieldValue(FILED_TO_REPEAT, repeated);
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
            Map<String, Object> config
        ) {
            return new ExampleRepeatProcessor(tag, description);
        }
    }
}
