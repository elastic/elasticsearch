package org.elasticsearch.ingest.processor.meta;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.ingest.IngestDocument.*;

public class MetaDataProcessorTests extends ESTestCase {

    public void testExecute() throws Exception {
        Map<IngestDocument.MetaData, Mustache> templates = new HashMap<>();
        for (MetaData metaData : MetaData.values()) {
            templates.put(metaData, new DefaultMustacheFactory().compile(new FastStringReader("some {{field}}"), "noname"));
        }

        MetaDataProcessor processor = new MetaDataProcessor(templates);
        IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", Collections.singletonMap("field", "value"));
        processor.execute(ingestDocument);

        for (MetaData metaData : MetaData.values()) {
            assertThat(ingestDocument.getMetadata(metaData), Matchers.equalTo("some value"));
        }
    }

}
