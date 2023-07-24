/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.common.RerouteProcessor.DataStreamValueSource;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RerouteProcessorFactoryTests extends ESTestCase {

    public void testDefaults() throws Exception {
        RerouteProcessor processor = create(null, null);
        assertThat(processor.getDataStreamDataset(), equalTo(List.of(DataStreamValueSource.DATASET_VALUE_SOURCE)));
        assertThat(processor.getDataStreamNamespace(), equalTo(List.of(DataStreamValueSource.NAMESPACE_VALUE_SOURCE)));
    }

    public void testInvalidDataset() throws Exception {
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> create("my-service", null));
        assertThat(e.getMessage(), equalTo("[dataset] 'my-service' contains disallowed characters"));
    }

    public void testInvalidNamespace() throws Exception {
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> create("generic", "foo:bar"));
        assertThat(e.getMessage(), equalTo("[namespace] 'foo:bar' contains disallowed characters"));
    }

    public void testDestinationSuccess() throws Exception {
        RerouteProcessor processor = create(Map.of("destination", "foo"));
        assertThat(processor.getDestination(), equalTo("foo"));
    }

    public void testDestinationAndDataset() {
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> create(Map.of("destination", "foo", "dataset", "bar"))
        );
        assertThat(e.getMessage(), equalTo("[destination] can only be set if dataset and namespace are not set"));
    }

    public void testFieldReference() throws Exception {
        create("{{foo}}", "{{{bar}}}");
    }

    public void testInvalidFieldReference() throws Exception {
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> create("{{foo}}-{{bar}}", "foo"));
        assertThat(e.getMessage(), equalTo("[dataset] '{{foo}}-{{bar}}' is not a valid field reference"));

        e = expectThrows(ElasticsearchParseException.class, () -> create("{{{{foo}}}}", "foo"));
        assertThat(e.getMessage(), equalTo("[dataset] '{{{{foo}}}}' is not a valid field reference"));
    }

    private static RerouteProcessor create(String dataset, String namespace) throws Exception {
        Map<String, Object> config = new HashMap<>();
        if (dataset != null) {
            config.put("dataset", dataset);
        }
        if (namespace != null) {
            config.put("namespace", namespace);
        }
        return create(config);
    }

    private static RerouteProcessor create(Map<String, Object> config) throws Exception {
        return new RerouteProcessor.Factory().create(null, null, null, new HashMap<>(config));
    }
}
