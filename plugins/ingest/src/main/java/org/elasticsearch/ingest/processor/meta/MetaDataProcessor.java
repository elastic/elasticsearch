package org.elasticsearch.ingest.processor.meta;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestDocument.MetaData;
import org.elasticsearch.ingest.processor.Processor;

import java.io.StringWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public final class MetaDataProcessor implements Processor {

    public final static String TYPE = "meta";

    private final Map<MetaData, Mustache> templates;

    public MetaDataProcessor(Map<MetaData, Mustache> templates) {
        this.templates = templates;
    }

    @Override
    public void execute(IngestDocument ingestDocument) {
        Map<String, Object> model = ingestDocument.getSource();
        for (Map.Entry<MetaData, Mustache> entry : templates.entrySet()) {
            StringWriter writer = new StringWriter();
            entry.getValue().execute(writer, model);
            ingestDocument.setEsMetadata(entry.getKey(), writer.toString());
        }
    }

    @Override
    public String getType() {
        return TYPE;
    }

    Map<MetaData, Mustache> getTemplates() {
        return templates;
    }

    public final static class Factory implements Processor.Factory<MetaDataProcessor> {

        private final MustacheFactory mustacheFactory = new DefaultMustacheFactory();

        @Override
        public MetaDataProcessor create(Map<String, Object> config) throws Exception {
            Map<MetaData, Mustache> templates = new HashMap<>();
            Iterator<Map.Entry<String, Object>> iterator = config.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                MetaData metaData = MetaData.fromString(entry.getKey());
                Mustache mustache = mustacheFactory.compile(new FastStringReader(entry.getValue().toString()), "");
                templates.put(metaData, mustache);
                iterator.remove();
            }

            if (templates.isEmpty()) {
                throw new IllegalArgumentException("no meta fields specified");
            }

            return new MetaDataProcessor(Collections.unmodifiableMap(templates));
        }
    }

}
