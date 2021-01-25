/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import java.util.HashMap;
import java.util.Map;
import static org.hamcrest.Matchers.equalTo;

public class DetectMimeTypeProcessorTests extends ESTestCase {
    public void testBasic() throws Exception {
        testMimeTypeProcessor(false, "{}", "application/json");
        testMimeTypeProcessor(false, "<test></test>", "text/xml");
        testMimeTypeProcessor(false, "<html>Test</html>", "text/html");
        testMimeTypeProcessor(false, "Hello world!", "text/plain");
        testMimeTypeProcessor(true, "iVBORw0KGgoAAAANSUhEUgAAAlgAAAJYCAIAAAAxBA+LAAAABGdBTUEAALGPC/xhBQA=", "image/png");
        // binary chunks of pe, mach-o, and elf binaries
        testMimeTypeProcessor(true, "TVqQAAMAAAAEAAAA//8==", "application/vnd.microsoft.portable-executable");
        testMimeTypeProcessor(true, "z/rt/gcAAAEDAAAAAgAAABAAAABYBQAAhQAgAAAAAAAZAAAASAAAAF9fUEFHRVpFUk8=", "application/x-mach-binary");
        testMimeTypeProcessor(true, "f0VMRgEBAQAAAAAAAAAAAAMAAwABAAAA8NwBADQAAACAoxgAAAAAADQAIAAIACgAHgAdAAE=", "application/x-sharedlib");
    }

    private void testMimeTypeProcessor(boolean isBase64, String data, String expectedType) throws Exception {
        var processor = new DetectMimeTypeProcessor(null, null, "data", "type", isBase64, true);
        Map<String, Object> source = new HashMap<>();
        source.put("data", data);
        IngestDocument input = new IngestDocument(source, Map.of());
        IngestDocument output = processor.execute(input);
        String hash = output.getFieldValue("type", String.class, true);
        assertThat(hash, equalTo(expectedType));
    }
}
